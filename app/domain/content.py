"""Identifying a file from its first bytes.

The `Content-Type` on an upload is supplied by whoever is uploading, so on its own it says
nothing. This looks at the actual header.

It is not a general-purpose file identifier and does not try to be: it recognises exactly
the types the allowlist permits, and returns "" for anything else. A short allowlist that is
easy to read beats a dependency on libmagic and a system library in every image.
"""

from __future__ import annotations

# How much of a file is needed to identify it. WEBP needs 12 bytes; nothing here needs more
# than 16.
SNIFF_BYTES = 32


def sniff(header: bytes) -> str:
    """Return the MIME type the bytes look like, or "" if unrecognised.

    Order matters: the container formats are checked before the generic ones, because a
    WEBP is a RIFF file and an MP4's signature sits at an offset rather than at the start.
    """
    if len(header) < 4:
        return ""

    # PNG: the 8-byte signature is deliberately chosen to survive text-mode transfers.
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"

    # JPEG: SOI marker. The third byte is always FF for a real JPEG.
    if header.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"

    if header.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"

    # RIFF container: WEBP is RIFF....WEBP, with a 4-byte length in between.
    if header.startswith(b"RIFF") and len(header) >= 12 and header[8:12] == b"WEBP":
        return "image/webp"

    # ISO base media file format. The brand lives at offset 4, after the box size, so an MP4
    # does not start with anything recognisable.
    if len(header) >= 12 and header[4:8] == b"ftyp":
        return "video/mp4"

    # Matroska/WEBM share the EBML header; WEBM is the profile the allowlist permits.
    if header.startswith(b"\x1a\x45\xdf\xa3"):
        return "video/webm"

    if header.startswith(b"%PDF-"):
        return "application/pdf"

    if header.startswith(b"7z\xbc\xaf\x27\x1c"):
        return "application/x-7z-compressed"

    # ZIP, including the empty and spanned variants. Note that this also matches every
    # modern Office document and JAR — which is exactly why the allowlist, not the sniffer,
    # decides what is acceptable.
    if header.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
        return "application/zip"

    return ""


def looks_like_text(header: bytes) -> bool:
    """Whether the header is plausibly UTF-8 text with no control bytes.

    Plain text has no signature, so it is identified by exclusion. A null byte anywhere in
    the header is the giveaway that something binary is claiming to be text.
    """
    if not header:
        return False
    if b"\x00" in header:
        return False
    try:
        decoded = header.decode("utf-8")
    except UnicodeDecodeError:
        # A truncated multi-byte character at the boundary is expected and not a failure,
        # so a little slack is allowed at the end.
        try:
            decoded = header[:-3].decode("utf-8")
        except UnicodeDecodeError:
            return False
    return all(ch.isprintable() or ch in "\r\n\t" for ch in decoded)


def identify(header: bytes, declared_type: str) -> str:
    """The type the bytes look like, with text handled by exclusion.

    ``text/plain`` is the one allowed type with no magic number, so it is only accepted when
    the caller claims it *and* the header contains nothing binary. Otherwise a null-byte-free
    executable would pass as text.
    """
    sniffed = sniff(header)
    if sniffed:
        return sniffed
    if declared_type.split(";")[0].strip().lower() == "text/plain" and looks_like_text(header):
        return "text/plain"
    return ""
