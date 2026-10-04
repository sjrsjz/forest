import io

from PIL import Image

from forest.media_capture_io import _validate


def test_animated_image_reports_all_decoded_frame_pixels():
    first = Image.new("RGB", (3, 2), "red")
    second = Image.new("RGB", (3, 2), "blue")
    stream = io.BytesIO()
    first.save(stream, format="GIF", save_all=True, append_images=[second])

    captured = _validate(stream.getvalue())

    assert captured.media_type == "image/gif"
    assert (captured.width, captured.height) == (3, 2)
    assert captured.decoded_pixels == 12
