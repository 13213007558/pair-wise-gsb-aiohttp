.. currentmodule:: aiohttp

.. _aiohttp-multipart-reference:

Multipart reference
===================

.. class:: MultipartResponseWrapper(resp, stream)
   :canonical: aiohttp.multipart.MultipartResponseWrapper

   Wrapper around the :class:`MultipartReader` to take care about
   underlying connection and close it when it needs in.


   .. method:: at_eof()

      Returns ``True`` when all response data had been read.

      :rtype: bool

   .. method:: next()
      :async:

      Emits next multipart reader object.

   .. method:: release()
      :async:

      Releases the connection gracefully, reading all the content
      to the void.


.. class:: BodyPartReader(boundary, headers, content)
   :canonical: aiohttp.multipart.BodyPartReader

   Multipart reader for single body part.

   The reader enforces the byte budgets configured on the parent
   :class:`MultipartReader` (or passed directly): *client_max_size* caps
   the raw bytes of a single part, *max_part_size* caps its decoded
   bytes.  Both raw and decoded counters are shared by :meth:`read`,
   :meth:`read_chunk`, :meth:`readline`, :meth:`release`, :meth:`text`,
   :meth:`json`, :meth:`form`, :meth:`decode` and :meth:`decode_iter`,
   so draining an unconsumed part cannot bypass the budgets.

   Once a read fails (malformed data, budget exceeded, cancellation or
   a disconnect), the error is latched and every further operation
   re-raises it: the reader never silently continues from an undefined
   stream position.

   .. method:: read(*, decode=False)
      :async:

      Reads body part data.

      :param bool decode: Decodes data following by encoding method
                          from ``Content-Encoding`` header. If it
                          missed data remains untouched

      :rtype: bytearray

   .. method:: read_chunk(size=chunk_size)
      :async:

      Reads body part content chunk of the specified size.

      :param int size: chunk size

      :rtype: bytearray

   .. method:: readline()
      :async:

      Reads body part by line by line.

      :rtype: bytearray

   .. method:: release()
      :async:

      Like :meth:`read`, but reads all the data to the void.

      :rtype: None

   .. method:: text(*, encoding=None)
      :async:

      Like :meth:`read`, but assumes that body part contains text data.

      :param str encoding: Custom text encoding. Overrides specified
                           in charset param of ``Content-Type`` header

      :rtype: str

   .. method:: json(*, encoding=None)
      :async:

      Like :meth:`read`, but assumes that body parts contains JSON data.

      :param str encoding: Custom JSON encoding. Overrides specified
                           in charset param of ``Content-Type`` header

   .. method:: form(*, encoding=None)
      :async:

      Like :meth:`read`, but assumes that body parts contains form
      urlencoded data.

      :param str encoding: Custom form encoding. Overrides specified
                           in charset param of ``Content-Type`` header

   .. method:: at_eof()

      Returns ``True`` if the boundary was reached or ``False`` otherwise.

      :rtype: bool

   .. method:: decode(data)

      Decodes data synchronously according the specified ``Content-Encoding``
      or ``Content-Transfer-Encoding`` headers value.

      Supports ``gzip``, ``deflate`` and ``identity`` encodings for
      ``Content-Encoding`` header.

      Supports ``base64``, ``quoted-printable``, ``binary`` encodings for
      ``Content-Transfer-Encoding`` header.

      :param bytearray data: Data to decode.

      :raises: :exc:`RuntimeError` - if encoding is unknown.

      :rtype: bytes

      .. note::

         For large payloads, consider using :meth:`decode_iter` instead
         to avoid blocking the event loop during decompression.

   .. method:: decode_iter(data)
      :async:

      Decodes data asynchronously according the specified ``Content-Encoding``
      or ``Content-Transfer-Encoding`` headers value.

      This is an async iterator and will return decoded data in chunks. This
      can be used to avoid loading large payloads into memory.

      This method offloads decompression to an executor for large payloads
      to avoid blocking the event loop.

      Supports ``gzip``, ``deflate`` and ``identity`` encodings for
      ``Content-Encoding`` header.

      Supports ``base64``, ``quoted-printable``, ``binary`` encodings for
      ``Content-Transfer-Encoding`` header.

      :param bytearray data: Data to decode.

      :raises: :exc:`RuntimeError` - if encoding is unknown.

      :rtype: bytes

      .. versionadded:: 3.13.4

   .. method:: get_charset(default=None)

      Returns charset parameter from ``Content-Type`` header or default.

   .. attribute:: name

      A field *name* specified in ``Content-Disposition`` header or ``None``
      if missed or header is malformed.

      Readonly :class:`str` property.

   .. attribute:: filename

      A field *filename* specified in ``Content-Disposition`` header or ``None``
      if missed or header is malformed.

      Readonly :class:`str` property.


.. class:: MultipartReader(headers, content)
   :canonical: aiohttp.multipart.MultipartReader

   Multipart body reader.

   :param headers: *multipart/* response/request headers mapping.
   :param content: :class:`~aiohttp.StreamReader` with the body.
   :param int client_max_size: maximum raw (still encoded) bytes of a
                               single body part, unlimited by default.
   :param int max_depth: maximum nesting depth of *multipart/* parts;
                         the outermost body is depth ``1`` and the
                         default is ``10``.
   :param int max_fields: maximum total number of parts across the
                          whole (possibly nested) body, unlimited by
                          default.
   :param int max_part_size: maximum decoded bytes of a single body
                             part, unlimited by default.
   :param int max_total_size: maximum decoded bytes summed over all
                              parts of the whole body, unlimited by
                              default.
   :param max_size_error_cls: exception class raised when any of the
                              budgets above is exceeded;
                              :class:`MultipartLimitExceeded` by default
                              and
                              :exc:`~aiohttp.web_exceptions.HTTPRequestEntityTooLarge`
                              on the server side.

   Exceeding a budget raises *max_size_error_cls*; malformed bodies
   raise :class:`MultipartFormatError`; a client disconnect or a
   cancellation propagates as the original stream error.  After any of
   these the reader is closed: further calls re-raise the latched
   error instead of reading from an undefined stream position.

   .. versionchanged:: 4.0

      Added the *max_depth*, *max_fields*, *max_part_size* and
      *max_total_size* parameters.

   .. classmethod:: from_response(cls, response, **kwargs)

      Constructs reader instance from HTTP response.

      :param response: :class:`~aiohttp.ClientResponse` instance
      :param kwargs: passed through to the reader constructor, e.g.
                      ``max_depth`` or ``max_total_size``.

      .. versionchanged:: 4.0

         Added *kwargs* pass-through.

   .. method:: at_eof()

      Returns ``True`` if the final boundary was reached or
      ``False`` otherwise.

      :rtype: bool

   .. method:: next()
      :async:

      Emits the next multipart body part.

   .. method:: release()
      :async:

      Reads all the body parts to the void till the final boundary.

   .. method:: fetch_next_part()
      :async:

      Returns the next body part reader.


.. class:: MultipartError
   :canonical: aiohttp.multipart.MultipartError

   Base class for multipart reader errors.

   .. versionadded:: 4.0


.. class:: MultipartFormatError
   :canonical: aiohttp.multipart.MultipartFormatError

   The multipart body is malformed: bad or missing boundaries, invalid
   per-part ``Content-Length``, malformed transfer-encoded data or an
   unknown content encoding.  Subclasses :exc:`ValueError`.

   .. versionadded:: 4.0


.. class:: MultipartLimitExceeded
   :canonical: aiohttp.multipart.MultipartLimitExceeded

   A configured multipart reader budget (depth, field count, raw or
   decoded size) was exceeded.  Subclasses :exc:`ValueError`.

   .. attribute:: limit

      The configured limit that was exceeded (:class:`int`).

   .. attribute:: kind

      Which budget was exceeded (:class:`str`): ``"size"``,
      ``"part size"``, ``"total size"``, ``"field count"`` or
      ``"depth"``.

   .. versionadded:: 4.0


.. class:: MultipartWriter(subtype='mixed', boundary=None, close_boundary=True)
   :canonical: aiohttp.multipart.MultipartWriter

   Multipart body writer.

   ``boundary`` may be an ASCII-only string.

   .. attribute:: boundary

      The string (:class:`str`) representation of the boundary.

      .. versionchanged:: 3.0

         Property type was changed from :class:`bytes` to :class:`str`.

   .. method:: append(obj, headers=None)

      Append an object to writer.

   .. method:: append_payload(payload)

      Adds a new body part to multipart writer.

   .. method:: append_json(obj, headers=None)

      Helper to append JSON part.

   .. method:: append_form(obj, headers=None)

      Helper to append form urlencoded part.

   .. attribute:: size

      Size of the payload.

   .. method:: write(writer, close_boundary=True)
      :async:

      Write body.

      :param bool close_boundary: The (:class:`bool`) that will emit
                                  boundary closing. You may want to disable
                                  when streaming (``multipart/x-mixed-replace``)

      .. versionadded:: 3.4

         Support ``close_boundary`` argument.
