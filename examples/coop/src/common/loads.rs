//! The **"dynamically loaded artifact(s)"** banner — the CLI analogue of the browser
//! showcases' "what you're seeing" concepts box, for the artifact-library demos that pull a
//! component or model **at runtime** (`catalog` · `mcp_toolgrowth` · `provisioning` ·
//! `model_deploy` · `reheal_deploy`).
//!
//! Each such demo declares a `LOADS: &[Loads]` const and calls [`announce_loads`] at startup, so
//! a viewer sees — up front — **what** the content is, **its type**, and **where it is loaded
//! from**. The same three facts are mirrored in the example's `## Loads` doc-comment block.

/// One artifact a demo pulls dynamically at runtime.
pub struct Loads {
    /// What the content is (e.g. `"route/optimize — a WASM component (echo fixture)"`).
    pub content: &'static str,
    /// Its artifact type (e.g. `"ArtifactKind::WasmComponent"` / `"ArtifactKind::Blob (GGUF weights)"`).
    pub kind: &'static str,
    /// Where it is loaded from — the source path (origin → catalogue → transport).
    pub from: &'static str,
}

const WIDTH: usize = 64;
const WRAP: usize = 58;

/// Print the standardized banner. Left-bordered only (no right border), with the `from`
/// path word-wrapped under its label, so long source paths never break the box. The top and
/// bottom rules are the same width regardless of the title's length.
pub fn announce_loads(items: &[Loads]) {
    let plural = if items.len() > 1 { "s" } else { "" };
    let head = format!("┌─ dynamically loaded artifact{plural} ");
    println!("{head}{}", "─".repeat(WIDTH.saturating_sub(head.chars().count())));
    for (i, it) in items.iter().enumerate() {
        if i > 0 {
            println!("│");
        }
        println!("│ Content · {}", it.content);
        println!("│ Type    · {}", it.kind);
        wrapped("From    · ", it.from);
    }
    println!("└{}", "─".repeat(WIDTH - 1));
    // Flush now so the banner is visible immediately, even when stdout is piped
    // (block-buffered) rather than a tty (line-buffered).
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

/// Print `label` + `text`, wrapping `text` at [`WRAP`] columns and indenting continuations
/// to align under the text (past the label), each line carrying the `│` border.
fn wrapped(label: &str, text: &str) {
    let indent = " ".repeat(label.chars().count());
    let mut line = String::new();
    let mut first = true;
    for word in text.split_whitespace() {
        if !line.is_empty() && line.chars().count() + 1 + word.chars().count() > WRAP {
            let lead = if first { label } else { indent.as_str() };
            println!("│ {lead}{line}");
            first = false;
            line.clear();
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() {
        let lead = if first { label } else { indent.as_str() };
        println!("│ {lead}{line}");
    }
}
