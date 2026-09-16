import unittest

from utils.network_config import (
    FL_SERVER_PORT_MAX,
    FL_SERVER_PORT_MIN,
    iter_fl_server_ports,
)


class NetworkConfigTest(unittest.TestCase):
    def test_fl_server_port_range(self):
        ports = list(iter_fl_server_ports())

        self.assertEqual(FL_SERVER_PORT_MIN, 40026)
        self.assertEqual(FL_SERVER_PORT_MAX, 40039)
        self.assertEqual(ports[0], 40026)
        self.assertEqual(ports[-1], 40039)
        self.assertEqual(len(ports), 14)


if __name__ == "__main__":
    unittest.main()
