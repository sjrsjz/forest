"""Package-declared immutable JSON documents used by configuration fields."""
from __future__ import annotations

import inspect
import json
from copy import deepcopy

from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.packages import digest_json


class ConfigurationDocuments:
    def __init__(self, repository, declarations, schema_loader, configuration_schema, validator=None):
        self.repository = repository
        self.declarations = tuple(declarations)
        self.schemas = {item['field']: schema_loader(item['schema']) for item in self.declarations}
        self.configuration_schema = Draft202012Validator(configuration_schema)
        self.validator = validator

    async def read(self, configuration):
        result = {}
        async with self.repository.store.pool.connection() as conn, conn.transaction():
            for item in self.declarations:
                field = item['field']
                result[field] = {'max_bytes': item['max_bytes']}
                for head in ('desired', 'effective'):
                    ref = configuration[head]['value'][field]
                    await self.repository.payloads.verify_in(
                        conn, **self._reference(item, ref))
                    content = await self.repository.payloads.read_bytes_in(conn, ref['payload_id'])
                    result[field][head] = self._decode(item, content)
        return result

    def _reference(self, item, ref):
        return {'payload_id': ref['payload_id'], 'digest': ref['digest'],
                'owner_component_id': self.repository.component_id,
                'owner_kind': item['owner_kind'], 'owner_id': ref['owner_id']}

    def _decode(self, item, content):
        if len(content) > item['max_bytes']:
            raise ForestError('CONFIGURATION_DOCUMENT_TOO_LARGE', 'Configuration document exceeds its byte limit', 422)
        try:
            value = json.loads(content)
        except (ValueError, UnicodeError):
            raise ForestError('CONFIGURATION_DOCUMENT_INVALID', 'Configuration document is not JSON', 422) from None
        if not Draft202012Validator(self.schemas[item['field']]).is_valid(value):
            raise ForestError('CONFIGURATION_DOCUMENT_INVALID', 'Configuration document violates its package schema', 422)
        return value

    async def materialize_in(self, conn, value):
        if not isinstance(value, dict):
            raise ForestError('MODULE_CONFIGURATION_INVALID', 'Configuration must be an object', 422)
        normalized, parents = deepcopy(value), []
        for item in self.declarations:
            field = item['field']
            document = normalized.get(field)
            if isinstance(document, dict) and set(document) == {'$document'}:
                try:
                    content = json.dumps(document['$document'], ensure_ascii=False, sort_keys=True,
                                         separators=(',', ':'), allow_nan=False).encode('utf-8')
                except (ValueError, TypeError, UnicodeError):
                    raise ForestError('CONFIGURATION_DOCUMENT_INVALID', 'Configuration document is not valid JSON', 422) from None
                self._decode(item, content)
                digest = digest_json(document['$document'])
                owner_id = item['owner_prefix'] + digest[7:]
                saved = await self.repository.payloads.put_bytes_in(
                    conn, owner_component_id=self.repository.component_id,
                    owner_kind=item['owner_kind'], owner_id=owner_id,
                    media_type='application/json', content=content,
                    expected_deletion_epoch=await self.repository.payloads.epoch_in(conn),
                )
                normalized[field] = {'payload_id': saved.payload_id, 'digest': saved.content_digest, 'owner_id': owner_id}
                normalized[item['revision_field']] = digest
        if not self.configuration_schema.is_valid(normalized):
            raise ForestError('MODULE_CONFIGURATION_INVALID', 'Configuration violates its package schema', 422)
        for item in self.declarations:
            ref = normalized[item['field']]
            await self.repository.payloads.verify_in(conn, **self._reference(item, ref))
            content = await self.repository.payloads.read_bytes_in(conn, ref['payload_id'])
            self._decode(item, content)
            parents.append(ref['payload_id'])
        if self.validator is not None:
            validated = self.validator(normalized)
            if inspect.isawaitable(validated):
                await validated
        return normalized, tuple(dict.fromkeys(parents))
