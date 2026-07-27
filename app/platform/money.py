"""Exact money.

Every amount is an integer count of the currency's minor unit. A float cannot
represent 0.1 exactly, and Decimal still leaves the question of where rounding
happens; an integer leaves no question at all.

Over the wire ``amount_minor`` is a **string**, matching the wallet service, because
a JavaScript client silently truncates integers above 2**53.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema


class MoneyError(ValueError):
    """A money value or operation that cannot be represented."""


@dataclass(frozen=True, slots=True)
class Money:
    """An amount in the minor unit of a currency."""

    minor: int
    currency: str

    def __post_init__(self) -> None:
        if not isinstance(self.minor, int) or isinstance(self.minor, bool):
            raise MoneyError(f"amount must be an integer number of minor units, got {self.minor!r}")
        code = self.currency.strip().upper()
        if len(code) != 3 or not code.isalpha():
            raise MoneyError(f"currency must be a 3-letter ISO-4217 code, got {self.currency!r}")
        object.__setattr__(self, "currency", code)

    # --- construction ---------------------------------------------------

    @classmethod
    def zero(cls, currency: str) -> Money:
        return cls(0, currency)

    # --- arithmetic -----------------------------------------------------
    #
    # Adding two currencies is a bug, not a conversion, so it raises.

    def _same_currency(self, other: Money) -> None:
        if self.currency != other.currency:
            raise MoneyError(f"cannot combine {self.currency} with {other.currency}")

    def __add__(self, other: Money) -> Money:
        self._same_currency(other)
        return Money(self.minor + other.minor, self.currency)

    def __sub__(self, other: Money) -> Money:
        self._same_currency(other)
        return Money(self.minor - other.minor, self.currency)

    def __lt__(self, other: Money) -> bool:
        self._same_currency(other)
        return self.minor < other.minor

    def __le__(self, other: Money) -> bool:
        self._same_currency(other)
        return self.minor <= other.minor

    def __gt__(self, other: Money) -> bool:
        self._same_currency(other)
        return self.minor > other.minor

    def __ge__(self, other: Money) -> bool:
        self._same_currency(other)
        return self.minor >= other.minor

    @property
    def is_zero(self) -> bool:
        return self.minor == 0

    @property
    def is_positive(self) -> bool:
        return self.minor > 0

    @property
    def is_negative(self) -> bool:
        return self.minor < 0

    # --- rates ----------------------------------------------------------

    def basis_points(self, bps: int) -> Money:
        """Return bps/10000 of this amount, rounded half-up.

        Rates are basis points rather than percentages so that 0.5% is 50 — an
        integer — instead of a float nobody can round predictably. 1% = 100 bps.
        """
        if bps < 0:
            raise MoneyError(f"basis points must not be negative, got {bps}")
        # Half-up on the absolute value, so -3 and +3 round symmetrically.
        sign = -1 if self.minor < 0 else 1
        scaled = abs(self.minor) * bps
        return Money(sign * ((scaled + 5000) // 10_000), self.currency)

    def allocate(self, ratios: list[int]) -> list[Money]:
        """Split this amount by ratios, losing and inventing nothing.

        Largest-remainder allocation: the shares always add back up to the
        original amount, whatever the amount. This is what makes the 70/30
        revenue split safe for a price of 1 minor unit as well as for a
        realistic one.
        """
        if not ratios:
            raise MoneyError("allocate needs at least one ratio")
        if any(r < 0 for r in ratios):
            raise MoneyError("allocate ratios must not be negative")
        total_ratio = sum(ratios)
        if total_ratio == 0:
            raise MoneyError("allocate ratios must not all be zero")

        sign = -1 if self.minor < 0 else 1
        amount = abs(self.minor)

        shares = [amount * r // total_ratio for r in ratios]
        remainder = amount - sum(shares)

        # Hand the leftover units to the largest fractional parts first, breaking
        # ties by position so the result is deterministic.
        fractions = sorted(
            range(len(ratios)),
            key=lambda i: (-(amount * ratios[i] % total_ratio), i),
        )
        for i in fractions[:remainder]:
            shares[i] += 1

        return [Money(sign * s, self.currency) for s in shares]

    # --- representation -------------------------------------------------

    def __str__(self) -> str:
        return f"{self.minor} {self.currency}"

    def to_wire(self) -> dict[str, str]:
        return {"amount_minor": str(self.minor), "currency": self.currency}

    @classmethod
    def from_wire(cls, raw: Any) -> Money:
        if isinstance(raw, Money):
            return raw
        if not isinstance(raw, dict):
            raise MoneyError(f"money must be an object, got {type(raw).__name__}")
        amount = raw.get("amount_minor", raw.get("amount", 0))
        try:
            minor = int(str(amount))
        except (TypeError, ValueError) as exc:
            raise MoneyError(f"amount_minor {amount!r} is not an integer") from exc
        currency = raw.get("currency") or ""
        return cls(minor, currency)

    # --- pydantic integration -------------------------------------------

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: type[Any], handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls.from_wire,
            serialization=core_schema.plain_serializer_function_ser_schema(lambda m: m.to_wire()),
        )
