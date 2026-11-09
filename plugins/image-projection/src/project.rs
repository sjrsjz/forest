use crate::{Settings, contract::Error, media::Blob};
use image::{DynamicImage, ImageDecoder, ImageFormat, ImageReader, Limits, imageops::FilterType};
use std::io::Cursor;

pub enum Projection {
    Original { mime: String },
    Scaled(Blob),
}

fn reader<'a>(
    source: &'a Blob,
    settings: &Settings,
) -> Result<ImageReader<Cursor<&'a [u8]>>, Error> {
    let mut reader = ImageReader::new(Cursor::new(source.bytes.as_slice()))
        .with_guessed_format()
        .map_err(|e| Error::Image(e.to_string()))?;
    let mut limits = Limits::default();
    limits.max_alloc = Some(settings.decode_memory_mib as u64 * 1024 * 1024);
    reader.limits(limits);
    Ok(reader)
}

/// Original bytes remain in Media; the result is only a model-input view.
pub fn project(source: &Blob, settings: &Settings) -> Result<Projection, Error> {
    let reader = reader(source, settings)?;
    let format = reader
        .format()
        .ok_or_else(|| Error::Image("unrecognized image format".into()))?;
    let (width, height) = reader
        .into_dimensions()
        .map_err(|e| Error::Image(e.to_string()))?;
    let side = settings.max_side as u32;
    if width <= side && height <= side {
        return Ok(Projection::Original {
            mime: format.to_mime_type().into(),
        });
    }
    Ok(Projection::Scaled(thumbnail(source, settings)?))
}

/// Always encode a static frame. Small dimensions alone cannot bound the bytes
/// in an original animation or image container. Do not upscale small images.
pub fn thumbnail(source: &Blob, settings: &Settings) -> Result<Blob, Error> {
    let reader = reader(source, settings)?;
    let format = reader
        .format()
        .ok_or_else(|| Error::Image("unrecognized image format".into()))?;
    let mut decoder = reader
        .into_decoder()
        .map_err(|e| Error::Image(e.to_string()))?;
    let mut limits = Limits::default();
    limits.max_alloc = Some(settings.decode_memory_mib as u64 * 1024 * 1024);
    limits
        .reserve(decoder.total_bytes())
        .map_err(|e| Error::Image(e.to_string()))?;
    decoder
        .set_limits(limits)
        .map_err(|e| Error::Image(e.to_string()))?;
    let orientation = decoder
        .orientation()
        .map_err(|e| Error::Image(e.to_string()))?;
    let mut image = DynamicImage::from_decoder(decoder).map_err(|e| Error::Image(e.to_string()))?;
    image.apply_orientation(orientation);
    let side = settings.max_side as u32;
    let image = if image.width() > side || image.height() > side {
        image.resize(side, side, FilterType::Triangle)
    } else {
        image
    };
    let mut bytes = Cursor::new(Vec::new());
    let mime = if format == ImageFormat::Jpeg {
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut bytes, 90)
            .encode_image(&image)
            .map_err(|e| Error::Image(e.to_string()))?;
        "image/jpeg"
    } else {
        image
            .write_to(&mut bytes, ImageFormat::Png)
            .map_err(|e| Error::Image(e.to_string()))?;
        "image/png"
    };
    Ok(Blob {
        mime: mime.into(),
        bytes: bytes.into_inner(),
    })
}
