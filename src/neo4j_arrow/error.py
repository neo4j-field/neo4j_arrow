"""
Neo4j Flight Service Errors
"""
from pyarrow.lib import ArrowException
from pyarrow.flight import FlightServerError

from typing import Union

KnownExceptions = Union[ArrowException, FlightServerError, Exception]


def interpret(e: KnownExceptions) -> KnownExceptions:
    """
    Try to figure out which exception occurred based on the server response.
    """
    message = "".join(e.args)
    if "ALREADY_EXISTS" in message:
        return AlreadyExists(message)
    elif "INVALID_ARGUMENT" in message:
        return InvalidArgument(message)
    elif "NOT_FOUND" in message:
        return NotFound(message)
    elif "INTERNAL" in message:
        return InternalError(message)
    elif "UNKNOWN" in message:
        # nb. this one is usually a FlightServerError
        return UnknownError(message)

    # give up
    return e


class Neo4jArrowException(Exception):
    """
    Base class for neo4j_arrow exceptions.
    """

    def __init__(self, message: str):
        self.message = message


class UnknownError(Neo4jArrowException):
    """
    We have no idea what is wrong :(
    """

    def __init__(self, message: str):
        # These errors have ugly stack traces often repeated. Try to beautify.
        # nb. In reality there's an embedded gRPC dict-like message, but let's
        # not introduce dict parsing here because that's a security issue.
        try:
            self.message = message.replace(r"\n", "\n").replace(r"\'", "'").splitlines()[-1]
        except Exception:
            self.message = message


class AlreadyExists(Neo4jArrowException):
    """
    The named graph or database already exists or an import with the name is
    already running. (Can't distinguish this easily without parsing the message
    body.)
    """

    pass


class InvalidArgument(Neo4jArrowException):
    """
    Either invalid entity or invalid action requested.
    """

    pass


class NotFound(Neo4jArrowException):
    """
    The requested import process could not be found.
    """

    pass


class InternalError(Exception):
    """
    Something bad happened on the server side :(
    """

    pass
