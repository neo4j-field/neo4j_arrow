"""
Neo4j Flight Service Errors
"""
from pyarrow.lib import ArrowException


def interpret(e: ArrowException) -> Exception:
    """
    Try to figure out which exception occcurred based on the server response.
    """
    try:
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
            return UnknownError(message)
    except:
        pass
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
    pass


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
