class ChronicleFlowError(Exception):
    code = "internal_error"
    status = 500


class ValidationError(ChronicleFlowError):
    code = "validation_error"
    status = 400


class NotFoundError(ChronicleFlowError):
    code = "not_found"
    status = 404


class ConflictError(ChronicleFlowError):
    code = "conflict"
    status = 409

