"""What may be stored, and what a file actually is.

This service is the platform's only file store, which makes it the platform's largest
attack surface for content. Every test here corresponds to a specific way that goes wrong.
"""

from __future__ import annotations

import pytest

from app.domain import content
from app.domain.media import (
    MAX_SIZE,
    MB,
    MediaKind,
    MediaObject,
    Visibility,
    object_key_for,
    sanitise_filename,
    validate_content,
    validate_size,
)
from app.platform import errors

# Real magic headers, so the sniffer is tested against what it will actually see.
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 24
JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 28
GIF = b"GIF89a" + b"\x00" * 26
WEBP = b"RIFF" + b"\x00\x01\x00\x00" + b"WEBP" + b"\x00" * 20
MP4 = b"\x00\x00\x00\x20ftypisom" + b"\x00" * 20
WEBM = b"\x1a\x45\xdf\xa3" + b"\x00" * 28
PDF = b"%PDF-1.7\n" + b"\x00" * 23
ZIP = b"PK\x03\x04" + b"\x00" * 28
HTML = b"<!DOCTYPE html><html><body>hi</body></html>"
ELF = b"\x7fELF\x02\x01\x01\x00" + b"\x00" * 24


# --- sniffing ------------------------------------------------------------


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (PNG, "image/png"),
        (JPEG, "image/jpeg"),
        (GIF, "image/gif"),
        (WEBP, "image/webp"),
        (MP4, "video/mp4"),
        (WEBM, "video/webm"),
        (PDF, "application/pdf"),
        (ZIP, "application/zip"),
    ],
)
def test_the_sniffer_recognises_each_allowed_type(header: bytes, expected: str):
    assert content.sniff(header) == expected


def test_the_sniffer_does_not_guess():
    """Anything unrecognised returns "" rather than a plausible answer.

    A sniffer that guesses is worse than one that admits it does not know: the allowlist can
    handle "unknown", but not a confident wrong answer.
    """
    assert content.sniff(HTML) == ""
    assert content.sniff(ELF) == ""
    assert content.sniff(b"") == ""
    assert content.sniff(b"ab") == ""


def test_a_webp_is_not_mistaken_for_a_bare_riff_file():
    """WEBP is a RIFF container, so the brand at offset 8 is what distinguishes it."""
    wav = b"RIFF" + b"\x00\x01\x00\x00" + b"WAVE" + b"\x00" * 20
    assert content.sniff(wav) == ""
    assert content.sniff(WEBP) == "image/webp"


def test_an_mp4_signature_is_found_at_its_offset():
    """An MP4 does not start with anything recognisable; the brand follows the box size."""
    assert content.sniff(MP4) == "video/mp4"


def test_text_is_identified_by_exclusion_only_when_claimed():
    """text/plain has no magic number, so it is accepted only if claimed *and* plausible."""
    assert content.identify(b"hello world\n", "text/plain") == "text/plain"
    # Not claimed, so not assumed.
    assert content.identify(b"hello world\n", "application/zip") == ""


def test_a_binary_cannot_pass_as_text():
    """A null byte in the header is the giveaway."""
    assert content.identify(ELF, "text/plain") == ""


def test_valid_utf8_text_is_accepted():
    assert content.identify("سلام دنیا\n".encode(), "text/plain") == "text/plain"


def test_a_truncated_multibyte_character_at_the_boundary_is_tolerated():
    """The header is a fixed-size prefix, so it can cut a character in half. That is normal,
    not a reason to reject the file."""
    header = ("x" * 30 + "€").encode()[: content.SNIFF_BYTES]
    assert content.identify(header, "text/plain") == "text/plain"


# --- the declared type is not trusted -----------------------------------


def test_an_html_page_declared_as_a_png_is_refused():
    """The attack this check exists for.

    Stored and served from our own origin, an HTML file becomes stored cross-site scripting.
    """
    with pytest.raises(errors.AppError) as caught:
        validate_content(
            kind=MediaKind.IMAGE,
            declared_type="image/png",
            sniffed_type=content.identify(HTML, "image/png"),
        )
    assert caught.value.reason == "CONTENT_TYPE_MISMATCH"


def test_an_executable_declared_as_an_image_is_refused():
    with pytest.raises(errors.AppError) as caught:
        validate_content(
            kind=MediaKind.IMAGE,
            declared_type="image/png",
            sniffed_type=content.identify(ELF, "image/png"),
        )
    assert caught.value.reason == "CONTENT_TYPE_MISMATCH"


def test_a_png_declared_as_a_jpeg_is_refused():
    """Also catches the honest mistake — a renamed file — with a message that says which."""
    with pytest.raises(errors.AppError) as caught:
        validate_content(kind=MediaKind.IMAGE, declared_type="image/jpeg", sniffed_type="image/png")
    assert caught.value.reason == "CONTENT_TYPE_MISMATCH"
    assert caught.value.details["actual_type"] == "image/png"


def test_a_type_outside_the_allowlist_is_refused_even_if_the_bytes_agree():
    with pytest.raises(errors.AppError) as caught:
        validate_content(
            kind=MediaKind.IMAGE, declared_type="image/svg+xml", sniffed_type="image/svg+xml"
        )
    assert caught.value.reason == "CONTENT_TYPE_NOT_ALLOWED"


def test_an_image_with_no_recognisable_signature_is_refused():
    """An image is expected to have a real signature, so "unknown" is not good enough."""
    with pytest.raises(errors.AppError) as caught:
        validate_content(kind=MediaKind.IMAGE, declared_type="image/png", sniffed_type="")
    assert caught.value.reason == "CONTENT_TYPE_MISMATCH"


def test_a_game_binary_may_be_an_unrecognised_archive():
    """An opaque blob is exactly what a build is, so the allowlist is open-ended here."""
    assert (
        validate_content(
            kind=MediaKind.GAME_BINARY,
            declared_type="application/octet-stream",
            sniffed_type="",
        )
        == "application/octet-stream"
    )


def test_the_type_stored_is_the_one_the_bytes_report():
    assert (
        validate_content(kind=MediaKind.IMAGE, declared_type="image/png", sniffed_type="image/png")
        == "image/png"
    )


def test_content_type_parameters_and_case_are_normalised():
    assert (
        validate_content(
            kind=MediaKind.IMAGE,
            declared_type="IMAGE/PNG; charset=binary",
            sniffed_type="image/png",
        )
        == "image/png"
    )


# --- size ---------------------------------------------------------------


def test_an_empty_upload_is_refused():
    with pytest.raises(errors.AppError) as caught:
        validate_size(kind=MediaKind.IMAGE, size_bytes=0)
    assert caught.value.reason == "MEDIA_EMPTY"


def test_an_oversized_image_is_refused():
    """One 40 GB upload fills a disk that four other services share."""
    with pytest.raises(errors.AppError) as caught:
        validate_size(kind=MediaKind.IMAGE, size_bytes=MAX_SIZE[MediaKind.IMAGE] + 1)
    assert caught.value.reason == "MEDIA_TOO_LARGE"
    assert caught.value.details["limit_bytes"] == MAX_SIZE[MediaKind.IMAGE]


def test_the_limits_differ_by_kind():
    """A 200 MB file is fine as a video and far too large as an image."""
    size = 200 * MB
    validate_size(kind=MediaKind.VIDEO, size_bytes=size)
    with pytest.raises(errors.AppError):
        validate_size(kind=MediaKind.IMAGE, size_bytes=size)


def test_a_file_exactly_at_the_limit_is_accepted():
    validate_size(kind=MediaKind.IMAGE, size_bytes=MAX_SIZE[MediaKind.IMAGE])


# --- visibility ---------------------------------------------------------


def test_an_image_defaults_to_public():
    """A storefront image behind a login is an image nobody sees."""
    media = _create(MediaKind.IMAGE, "image/png", PNG)
    assert media.visibility is Visibility.PUBLIC


def test_a_game_binary_defaults_to_private():
    media = _create(MediaKind.GAME_BINARY, "application/zip", ZIP)
    assert media.visibility is Visibility.PRIVATE


def test_a_game_binary_cannot_be_made_public():
    """The single worst bug this service could have.

    An unauthenticated URL for a build is a pirated copy, so the request cannot loosen it.
    """
    with pytest.raises(errors.AppError) as caught:
        _create(MediaKind.GAME_BINARY, "application/zip", ZIP, visibility=Visibility.PUBLIC)
    assert caught.value.reason == "VISIBILITY_NOT_ALLOWED"


def test_a_public_kind_can_be_made_private():
    """Tightening is always allowed — an unreleased screenshot, for instance."""
    media = _create(MediaKind.IMAGE, "image/png", PNG, visibility=Visibility.PRIVATE)
    assert media.visibility is Visibility.PRIVATE


# --- who may read -------------------------------------------------------


def test_anyone_may_read_a_public_object():
    media = _create(MediaKind.IMAGE, "image/png", PNG)
    assert media.readable_by(user_id="", is_staff=False, has_read_scope=False) is True


def test_a_stranger_may_not_read_a_private_object():
    media = _create(MediaKind.GAME_BINARY, "application/zip", ZIP)
    assert media.readable_by(user_id="stranger", is_staff=False, has_read_scope=False) is False


def test_the_owner_may_read_their_own_private_object():
    media = _create(MediaKind.GAME_BINARY, "application/zip", ZIP)
    assert media.readable_by(user_id="dev-1", is_staff=False, has_read_scope=False) is True


def test_a_service_with_the_read_scope_may_read_a_private_object():
    """How the catalog hands a build to a user who has actually bought the game."""
    media = _create(MediaKind.GAME_BINARY, "application/zip", ZIP)
    assert media.readable_by(user_id="catalog", is_staff=False, has_read_scope=True) is True


def test_nobody_may_read_a_deleted_object():
    media = _create(MediaKind.IMAGE, "image/png", PNG)
    media.delete(now=media.uploaded_at)
    assert media.readable_by(user_id="dev-1", is_staff=True, has_read_scope=True) is False


def test_deleting_twice_reports_that_nothing_changed():
    media = _create(MediaKind.IMAGE, "image/png", PNG)
    assert media.delete(now=media.uploaded_at) is True
    assert media.delete(now=media.uploaded_at) is False


def test_another_user_cannot_delete_someone_elses_file():
    media = _create(MediaKind.IMAGE, "image/png", PNG)
    with pytest.raises(errors.AppError) as caught:
        media.assert_owner("someone-else")
    assert caught.value.reason == "NOT_MEDIA_OWNER"


def test_staff_can_act_on_any_file():
    """Support removing reported content."""
    media = _create(MediaKind.IMAGE, "image/png", PNG)
    media.assert_owner("support-1", is_staff=True)


# --- filenames and keys -------------------------------------------------


def test_a_traversal_attempt_in_a_filename_is_reduced_to_its_basename():
    assert sanitise_filename("../../../etc/passwd") == ["etc", "passwd"][-1]
    assert sanitise_filename("..\\..\\windows\\system32\\cmd.exe") == "cmd.exe"


def test_control_characters_and_quotes_are_stripped_from_a_filename():
    """It ends up in a Content-Disposition header, where a quote would break out of the
    quoted string."""
    cleaned = sanitise_filename('evil"\x00\x1bname.png')
    assert '"' not in cleaned
    assert "\x00" not in cleaned
    assert "\x1b" not in cleaned


def test_a_very_long_filename_is_truncated():
    assert len(sanitise_filename("a" * 500 + ".png")) <= 255


def test_the_object_key_comes_from_the_id_and_nothing_else():
    """Which is why path traversal is an impossibility here, not a filter."""
    key = object_key_for("abcd1234")
    assert key == "ab/cd/abcd1234"
    assert ".." not in key


def test_the_object_key_ignores_anything_unsafe_in_an_id():
    assert ".." not in object_key_for("../../etc/passwd")
    assert "/" not in object_key_for("../..").split("/")[-1]


def test_a_short_id_still_produces_a_sharded_key():
    assert object_key_for("a") == "a0/00/a000"


# --- helpers ------------------------------------------------------------


def _create(
    kind: MediaKind, declared: str, data: bytes, *, visibility: Visibility | None = None
) -> MediaObject:
    from datetime import UTC, datetime

    return MediaObject.create(
        media_id="media-1",
        owner_id="dev-1",
        kind=kind,
        declared_type=declared,
        sniffed_type=content.identify(data[: content.SNIFF_BYTES], declared),
        size_bytes=len(data),
        filename="thing.bin",
        visibility=visibility,
        now=datetime(2026, 7, 27, 12, 0, tzinfo=UTC),
    )
