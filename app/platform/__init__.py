"""General-purpose plumbing with no business knowledge in it.

This is the code that would otherwise be scattered through the adapters: an exact
money type, the error taxonomy, the outbox machinery, JWT verification, the HTTP edge,
structured logging, SQL migrations.

It is vendored into each service rather than shared from one repository. A shared
module across five services in three languages would mean a path dependency to a
sibling checkout, a Docker build context spanning two repositories, and CI checking
out both — real, daily cost. When a fourth Python service arrives, publishing this
directory as a package is a short job, and at that point it can be a properly
versioned dependency instead of a copy.
"""
