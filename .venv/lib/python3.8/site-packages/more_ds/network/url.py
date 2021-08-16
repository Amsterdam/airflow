#  Copyright 2019 SURF.
#  Copyright 2021 Guido Kollerie
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from __future__ import annotations

from typing import Mapping
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


class URL(str):
    """Helper class for conveniently constructing URLs.

    To that end the ``/`` operator has been overloaded
    to append path elements.
    Similarly the ``//`` operator has been overloaded
    to easily add a query string to the :class:`URL`.

    Being a subclass of :class:`str`,
    instances of :class:`URL` can be used anywhere a :class:`str` is expected.

    *IMPORTANT* No form of verification is performed.
    Meaning for instance that any string,
    not only those that actually make up an :class:`URL`,
    can be use to initialize an :class:`URL` instance.
    And one can add multiple query strings to an :class:`URL` instance
    leading to an improperly formatted :class:`URL`.

    Example::

        >>> from more_ds.network import URL
        >>> base_url = URL("http://example.org/")
        >>> api_url = base_url / "api"
        >>> url = api_url / "ip" / "address" // dict(version=4)
        >>> print(url)
        http://example.org/api/ip/address?version=4

    """

    def __truediv__(self, path: object) -> URL:
        """Append path element to the URL object.

        It prevents accidental inclusion of too many slashes
        between the appended ``path`` elements
        should the :class:`URL` end in a slash
        and/or the ``path`` element start with a slash.

        Args:
            path: path element to append

        Returns:
            A new :class:`URL` object with the ``path`` element appended.

        """
        if not isinstance(path, str):
            path = str(path)
        return URL(self.rstrip("/") + "/" + path.lstrip("/"))

    def __floordiv__(self, query: Mapping) -> URL:
        """Append a query string to the URL.

        Args:
            query: Mapping of values that should be converted to a query string.

        Returns:
            a new :class:`URL` object with the ``query`` appended as a query string.
        """
        parts = urlparse(self)
        params = {**parse_qs(parts.query), **query}
        parts = parts._replace(query=urlencode(params, doseq=True))
        return URL(urlunparse(parts))

    def __repr__(self) -> str:
        """Return printable presentation of URL."""
        return f"URL('{self}')"
