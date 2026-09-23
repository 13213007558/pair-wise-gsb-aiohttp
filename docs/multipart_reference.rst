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

   .. method:: close()

      Closes the part without consuming the rest of its data.

      Marks the part as exhausted so that :meth:`read` and :meth:`release`
      become no-ops.  Because the underlying stream is left positioned
      somewhere inside the part, the parent :class:`MultipartReader`
      abandons the remainder of the message on its next :meth:`next()
      <MultipartReader.next>` call, and the connection cannot be reused.

      .. versionadded:: 4.0

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

   The reader accepts optional limits that are shared with every nested
   multipart reader and body part reader created from it.  All of them
   default to ``None`` (no limit).  A violated limit is raised with the
   ``max_size_error_cls`` exception class (``ValueError`` by default,
   :exc:`~aiohttp.web_exceptions.HTTPRequestEntityTooLarge` when
   constructed via :meth:`aiohttp.web_request.BaseRequest.multipart`),
   which keeps limit errors distinguishable from malformed-body
   ``ValueError``\ s and from connection errors raised by the stream:

   :param int | None max_depth: Maximum nesting depth of ``multipart/*``
      parts.  The top-level reader is depth 0, so ``max_depth=0`` rejects
      any nested multipart.
   :param int | None max_parts: Maximum total number of parts emitted
      across the whole (possibly nested) message.
   :param int | None max_part_raw_size: Maximum bytes read from the
      stream for a single part, before any decoding.
   :param int | None max_part_size: Maximum decoded bytes a single part
      may yield through :meth:`BodyPartReader.read` (also with
      ``decode=True``), :meth:`BodyPartReader.decode`,
      :meth:`BodyPartReader.decode_iter`, :meth:`BodyPartReader.text`,
      :meth:`BodyPartReader.json` and :meth:`BodyPartReader.form`.
   :param int | None max_total_raw_size: Maximum raw bytes accumulated
      over all parts of the message, including the parts drained by
      :meth:`release` or by :meth:`next` when a previous part was not
      fully consumed.
   :param int | None max_total_size: Maximum decoded bytes accumulated
      over all parts of the message.

   .. classmethod:: from_response(cls, response)

      Constructs reader instance from HTTP response.

      :param response: :class:`~aiohttp.ClientResponse` instance

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
