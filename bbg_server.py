# bbg_server.py
"""Tunneling a Bloomberg request through network
"""

import socketserver
import blpapi
import json
from typing import List, Dict, Any

def resolve(securities: List[str], fields: List[str]) -> Dict[str, Dict[str, Any]]:
    """Lookup securities data using Bloomberg Desktop API

    Args:
        securities: List of securities identifiers, e.g. ["ED1 Comdty", "ED2 Comdty"]
        fields: List of field names to look up for each of the securities, e.g.  ["fut_cur_gen_ticker", "PX_MID"]
    Returns:
        A dictionary of a form like the following:
          {
           "ED1 Comdty": {"fut_cur_gen_ticker": xxx, "PX_MID": yyy},
           "ED2 Comdty": {"fut_cur_gen_ticker": uuu, "PX_MID": vvv}
          }
    """
    # Set up bloomberg:
    # hard-coded server and port, desktop API can't do anything other than these
    soption = blpapi.SessionOptions()
    soption.setServerHost("localhost")
    soption.setServerPort(8194)
    soption.setAuthenticationOptions("AuthenticationType=OS_LOGON")
    session = blpapi.Session(soption)
    if not session.start():
        print("Cannot start blpapi session")
        return
    if not session.openService("//blp/refdata"):
        print("Cannot open //blp/refdata from session")
        return
    service = session.getService("//blp/refdata")

    # query bloomberg
    request = service.createRequest("ReferenceDataRequest")
    for security in securities:
        request.append("securities", security)
    for field in fields:
        request.append("fields", field)
    session.sendRequest(request)
    ret = {}
    try:
        while True:
            evt = session.nextEvent(500)
            if evt.eventType() not in [blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE]:
                continue # should not hit here
            for msg in evt:
                secarray = msg.getElement(blpapi.Name("securityData"))
                for secdata in secarray.values():
                    secname = secdata.getElementAsString(blpapi.Name("security"))
                    if secname not in ret:
                        ret[secname] = {}
                    fielddata = secdata.getElement(blpapi.Name("fieldData"))
                    for field in fielddata.elements():
                        if not field.isValid():
                            pass # ignore invalid fields
                        elif field.isArray():
                            ret[secname][str(field.name())] = list(field.values())
                        else:
                            ret[secname][str(field.name())] = field.getValueAsString()
            if evt.eventType() == blpapi.Event.RESPONSE:
                break # no more to read
    finally:
        session.stop()
    return ret

class ServerHandler(socketserver.StreamRequestHandler):
    """Syntax sugar to avoid socket.bind, socket.listen, and socket.accept calls
    """
    def handle(self):
        """Deal with the TCP socket at self.request once a client has connected"""
        # read data from client and parse
        data = self.rfile.read().decode("utf-8").strip()
        print("Received: %s" % data)
        securities = fields = None
        try:
            data = json.loads(data)
            if isinstance(data, dict):
                securities, fields = data["securities"], data["fields"]
            elif isinstance(data, list):
                securities, fields = data
            else:
                raise NotImplementedError
            assert isinstance(securities, list) and isinstance(fields, list)
            assert all([isinstance(x, str) for x in securities])
            assert all([isinstance(x, str) for x in fields])
        except json.JSONDecodeError:
            print("Non-JSON input data from remote")
            raise
        except NotImplementedError:
            print("Bad input data from remote: only list or dict are supported")
        except KeyError:
            print("Bad input data from remote: dict needs keys securities and fields")
        except ValueError:
            print("Bad input data from remote: list needs exactly two elements, securities and fields")
        except AssertionError:
            print("Bad input data from remote: securities and fields must be list of strings")
        if not securities or not fields:
            return # no input, skip query

        # query
        ret = resolve(securities, fields)

        # serialize and send it back to client
        data = json.dumps(ret)
        print("Return: %s" % data)
        self.wfile.write(data.encode("utf-8"))

def main(host, port):
    try:
        with socketserver.TCPServer((host, port), ServerHandler) as server:
            server.serve_forever()
    except KeyboardInterrupt:
        print("Server shutdown")

if __name__ == "__main__":
    main("localhost", 2600)
