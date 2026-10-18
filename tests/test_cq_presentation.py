import base64
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from forest.errors import ForestError
from forest.onebot_wire_effect_driver import OneBotWireEffectDriver

sys.path.insert(0, str(Path(__file__).parents[1] / 'src/forest/builtin/onebot_codec/python'))
from cq_presentation import prepare_template
from cq_memory import encode_message


def prepare(text):
    return prepare_template('group', '933179609', [{'type': 'text', 'format': 'platform-markup', 'text': text}])


def test_memory_cq_round_trip_preserves_message_order_and_literal_cq():
    parts = [
        {'type': 'reply', 'platform_message_id': '-7'},
        {'type': 'mention', 'external_user_id': '123'},
        {'type': 'text', 'text': ' A&B [CQ:at,qq=42] &amp;#91;'},
        {'type': 'face', 'face_id': '178'},
        {'type': 'image', 'media_ref': 'media:' + 'a' * 64, 'media_revision': 1,
         'source_unit_id': 'unit,1', 'part_id': 'part:image:4'},
    ]
    text = encode_message(parts)
    assert text.startswith('[CQ:reply,id=-7][CQ:at,qq=123] A&amp;B &#91;CQ:at,qq=42&#93;')
    wire = prepare(text)['body']['message']
    assert [part['type'] for part in wire] == ['reply', 'at', 'text', 'face', 'image']
    assert wire[2]['data']['text'] == parts[2]['text']
    assert wire[4]['data']['file']['media_reference']['source_unit_id'] == 'unit,1'


def test_memory_cq_does_not_copy_unsupported_media_data():
    text = encode_message([{'type': 'unsupported', 'platform_type': 'record',
                           'data': {'file': 'base64://' + 'a' * 50000}}])
    assert text == '[CQ:record]'


def test_cq_is_recognized_before_single_unescape_and_keeps_literal_text():
    value = prepare('A&amp;B &#91;CQ:at,qq=42&#93; &amp;#91; [CQ:at,qq=123][CQ:reply,id=-7][CQ:face,id=178]')
    assert value['body']['message'] == [
        {'type': 'text', 'data': {'text': 'A&B [CQ:at,qq=42] &#91; '}},
        {'type': 'at', 'data': {'qq': '123'}}, {'type': 'reply', 'data': {'id': '-7'}},
        {'type': 'face', 'data': {'id': '178'}},
    ]
    literal = prepare_template('group', '933179609', [{'type': 'text', 'text': '[CQ:at,qq=42] &amp;'}])
    assert literal['body']['message'] == [{'type': 'text', 'data': {'text': '[CQ:at,qq=42] &amp;'}}]


@pytest.mark.parametrize('text', ['[CQ:at,qq=1,qq=2]', '[CQ:at,qq=invalid]', '[CQ:image,file=file:///etc/passwd]', '[CQ:image,file=https://example.org/a.png]', '[CQ:json,data=whatever]', '[CQ:face, id=1]'])
def test_unsupported_or_malformed_cq_cannot_become_a_wire_action(text):
    assert prepare(text)['body']['message'] == [{'type': 'text', 'data': {'text': text}}]


def test_managed_image_parameter_entities_decode_only_once():
    value = prepare('[CQ:image,file=media:' + 'a' * 64 + ',revision=2,source_unit_id=unit&#44;1,part_id=part&amp;#91;]')
    ref = value['body']['message'][0]['data']['file']['media_reference']
    assert ref['source_unit_id'] == 'unit,1'
    assert ref['part_id'] == 'part&#91;'


@pytest.mark.asyncio
async def test_large_managed_image_uses_only_wire_references_across_worker_boundary():
    content = b'x' * (512 * 1024)
    ref = {'payload_id': 'image/1', 'digest': 'sha256:' + hashlib.sha256(content).hexdigest(),
           'owner_component_id': 'media.capture', 'owner_kind': 'media.asset', 'owner_id': 'media:' + 'a' * 64}
    payloads = SimpleNamespace(epoch=AsyncMock(return_value=3),
                               read_verified_batch=AsyncMock(return_value=(content,)),
                               put_bytes=AsyncMock(return_value=SimpleNamespace(payload_id='wire/1', content_digest='computed')))
    identity = SimpleNamespace(component_id='adapter.onebot', generation_id='generation/1')
    driver = OneBotWireEffectDriver(identity, runtime=SimpleNamespace(verify_generation_member=AsyncMock()), payloads=payloads)
    await driver.start()
    template = {'action': 'send_group_msg', 'wire_owner_id': 'attempt/1', 'body': {
        'group_id': 933179609, 'auto_escape': True, 'message': [{'type': 'image', 'data': {'file': {'payload': ref}}}],
    }}
    result = await driver.invoke(template, scope=identity, invocation=SimpleNamespace(operation_id='prepare/1'))
    assert len(json.dumps(result).encode()) < 1024
    written = payloads.put_bytes.call_args.kwargs
    assert len(written['content']) > 256 * 1024
    encoded = json.loads(written['content'])['message'][0]['data']['file']
    assert base64.b64decode(encoded.removeprefix('base64://')) == content
    assert written['parents'] == ('image/1',)
    assert template['body']['message'][0]['data']['file'] == {'payload': ref}
    payloads.read_verified_batch.side_effect = ForestError('PAYLOAD_REVOKED', 'revoked', 409)
    with pytest.raises(ForestError, match='revoked'):
        await driver.invoke(template, scope=identity, invocation=SimpleNamespace(operation_id='prepare/2'))
    assert payloads.put_bytes.await_count == 1
