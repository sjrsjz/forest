use crate::{Settings, contract::Error};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use typst::{
    Library, LibraryExt, World, WorldExt,
    diag::{FileError, FileResult, SourceDiagnostic},
    foundations::{Bytes, Datetime, Duration},
    syntax::{FileId, RootedPath, Source, VirtualPath, VirtualRoot},
    text::{Font, FontBook},
    utils::LazyHash,
};
use typst_layout::PagedDocument;

pub struct Fonts {
    book: LazyHash<FontBook>,
    faces: Vec<Font>,
    library: LazyHash<Library>,
}
impl Fonts {
    pub fn new() -> Self {
        let mut faces = Vec::new();
        for data in typst_assets::fonts().chain(std::iter::once(
            include_bytes!("../assets/NotoSansCJKsc-Regular.otf").as_slice(),
        )) {
            faces.extend(Font::iter(Bytes::new(data)));
        }
        Self {
            book: LazyHash::new(FontBook::from_fonts(&faces)),
            faces,
            library: LazyHash::new(Library::default()),
        }
    }
}

struct Document<'a> {
    fonts: &'a Fonts,
    source: Source,
    files: HashMap<FileId, Bytes>,
}
impl World for Document<'_> {
    fn library(&self) -> &LazyHash<Library> {
        &self.fonts.library
    }
    fn book(&self) -> &LazyHash<FontBook> {
        &self.fonts.book
    }
    fn main(&self) -> FileId {
        self.source.id()
    }
    fn source(&self, id: FileId) -> FileResult<Source> {
        if id == self.main() {
            Ok(self.source.clone())
        } else {
            Err(FileError::NotFound(std::path::PathBuf::from(
                id.vpath().get_with_slash(),
            )))
        }
    }
    fn file(&self, id: FileId) -> FileResult<Bytes> {
        if id == self.main() {
            return Ok(Bytes::from_string(self.source.text().to_owned()));
        }
        self.files.get(&id).cloned().ok_or_else(|| {
            FileError::NotFound(std::path::PathBuf::from(id.vpath().get_with_slash()))
        })
    }
    fn font(&self, index: usize) -> Option<Font> {
        self.fonts.faces.get(index).cloned()
    }
    fn today(&self, _: Option<Duration>) -> Option<Datetime> {
        None
    }
}

#[derive(Serialize)]
pub struct Diagnostic {
    message: String,
    line: Option<usize>,
    column: Option<usize>,
    hints: Vec<String>,
}
impl Document<'_> {
    fn diagnostic(&self, d: &SourceDiagnostic) -> Diagnostic {
        let offset = self.range(d.span).map(|r| r.start);
        // The three-line default prelude is not part of the caller's source.
        let line = offset
            .and_then(|n| self.source.lines().byte_to_line(n))
            .and_then(|n| n.checked_sub(3))
            .map(|n| n + 1);
        let column = offset
            .and_then(|n| self.source.lines().byte_to_column(n))
            .map(|n| n + 1);
        Diagnostic {
            message: d.message.to_string(),
            line,
            column,
            hints: d.hints.iter().map(|h| h.v.to_string()).collect(),
        }
    }
}
pub struct Page {
    pub bytes: Vec<u8>,
    pub width: u32,
    pub height: u32,
}

pub fn render(
    fonts: &Fonts,
    settings: &Settings,
    source: String,
    images: BTreeMap<String, Vec<u8>>,
) -> Result<(Vec<Page>, Vec<Diagnostic>), Error> {
    let source = format!(
        "#set page(width: {}pt, height: auto, margin: 16pt, fill: white)\n#set text(font: (\"Noto Sans CJK SC\", \"Libertinus Serif\"), size: {}pt)\n#set par(leading: 0.65em)\n{source}",
        settings.page_width_pt, settings.font_size_pt
    );
    let document = Document {
        fonts,
        source: Source::new(
            FileId::new(RootedPath::new(
                VirtualRoot::Project,
                VirtualPath::new("/main.typ").unwrap(),
            )),
            source,
        ),
        files: images
            .into_iter()
            .map(|(name, bytes)| {
                (
                    FileId::new(RootedPath::new(
                        VirtualRoot::Project,
                        VirtualPath::new(&name).expect("validated virtual image name"),
                    )),
                    Bytes::new(bytes),
                )
            })
            .collect(),
    };
    let compiled = typst::compile::<PagedDocument>(&document);
    let warnings = compiled
        .warnings
        .iter()
        .map(|d| document.diagnostic(d))
        .collect();
    let output = compiled.output.map_err(|errors| Error::Execution(serde_json::to_string(&serde_json::json!({"diagnostics": errors.iter().map(|d| document.diagnostic(d)).collect::<Vec<_>>()})).expect("diagnostic JSON")))?;
    if output.pages().len() > settings.max_pages as usize {
        return Err(Error::Execution(format!(
            "document exceeds {} pages",
            settings.max_pages
        )));
    }
    let scale = settings.pixels_per_point as f64;
    // Validate all page allocations before rendering or publishing any output.
    for page in output.pages() {
        let size = page.frame.size();
        let width = (size.x.to_pt() * scale).round().max(1.0);
        let height = (size.y.to_pt() * scale).round().max(1.0);
        if !width.is_finite()
            || !height.is_finite()
            || width * height > settings.max_page_pixels as f64
        {
            return Err(Error::Execution(format!(
                "page exceeds {} pixels; split or reduce page size",
                settings.max_page_pixels
            )));
        }
    }
    let options = typst_render::RenderOptions {
        pixel_per_pt: typst::utils::Scalar::new(scale),
        ..Default::default()
    };
    let mut pages = Vec::new();
    for page in output.pages() {
        let bitmap = typst_render::render(page, &options);
        pages.push(Page {
            width: bitmap.width(),
            height: bitmap.height(),
            bytes: bitmap
                .encode_png()
                .map_err(|e| Error::Execution(e.to_string()))?,
        });
    }
    Ok((pages, warnings))
}
