import os

FL_SERVER_PORT_MIN = int(os.getenv('FL_SERVER_PORT_MIN', '40026'))
FL_SERVER_PORT_MAX = int(os.getenv('FL_SERVER_PORT_MAX', '40039'))
if not 1 <= FL_SERVER_PORT_MIN <= FL_SERVER_PORT_MAX <= 65535:
    raise ValueError('FL_SERVER_PORT_MIN/MAX must define an ordered range in 1..65535')


def iter_fl_server_ports():
    """Return the TCP gateway ports reserved for Task FL servers."""
    return range(FL_SERVER_PORT_MIN, FL_SERVER_PORT_MAX + 1)
