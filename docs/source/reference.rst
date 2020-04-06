Reference
=========

Python API
----------

.. autofunction:: databroker_pack.export_catalog
.. autofunction:: databroker_pack.export_uids
.. autofunction:: databroker_pack.export_run
.. autofunction:: databroker_pack.copy_external_files
.. autofunction:: databroker_pack.unpack
.. autofunction:: databroker_pack.write_documents_manifest
.. autofunction:: databroker_pack.write_external_files_manifest
.. autofunction:: databroker_pack.write_jsonl_catalog_file
.. autofunction:: databroker_pack.write_msgpack_catalog_file

What kinds of files are in the "pack"?
--------------------------------------

Data Broker is emphatically not a "data store", but rather a Python library for
interacting with potentially *any* data store from a unified Python interface
that hands the user standard Python objects----dictionaries, arrays, and other
data structures widely used in the scientific Python ecosystems. It aims to
abstract over the necessary variety in file formats across different domains,
techniques, and instruments.

That said, it is sometimes necessary to take a look under the hood. The pack
directory always contains:

* Either msgpack (binary) or JSONL (plaintext) files containing the Bluesky
  `Documents <https://blueskyproject.io/event-model/data-model.html>`_.
* Text manifests listing the names of these files relative to the directory
  root. The manifests maybe split over multiple files named like
  ``documents_manfiest_N.txt`` to facilitate compressing and transferring in
  chunks.

If the Documents reference external files---typically large array data written
by detectors---these files may...

* Have their contents filled directly into the Documents, and thus included in
  the msgpack or JSONL. This is blunt but simple.
* Be listed in text manifests named like
  ``external_files_manfiest_HASH_N.txt``.  These manifests are suitable for
  feeding to tools to transfer large files in bulk, such as ``rsync`` or
  ``globus transfer --batch`` 
* Bundled into the pack directory in their original formats in directories
  named ``external_files/HASH/``.

The advantage of the first approach is that the recipient does not need special
I/O libraries installed to read the large array data. The advantage of the
second and third approaches is that loading the large array data can be
deferred.

The first and third approaches create self-contained directories, but the
second approach facilitates more efficient means of transferring large amounts
of data.
