===============
Release History
===============

v0.3.0 (2020-06-05)
-------------------

Changed
+++++++

* In the Python API, the :func:`databroker_pack.export_catalog` and
  :func:`databroker_pack.export_uids` return an additional item: a mapping of
  Run uids to the set of files associated with them---specifically, the
  ``(root, filepath)`` pairs.

v0.2.0 (2020-06-01)
-------------------

Added
+++++

* Separate pack directories can be unpacked into one catalog.
* A pack directory can unpacked into MongoDB.
* ``databroker-pack`` supports an optional ``--limit`` argument for
  conveniently exporting small test batches.
* ``databroker-pack`` supports an optional ``--salt`` argument for
  specifying a non-random salt.

Changed
+++++++

* ``databroker-unpacked`` has a new required argument, which must be
  ``inplace`` or ``mongo_normalized``.
* The internal directory structure has been changed. The files with the
  Documents are in ``documents/`` subdirectory, and the root hashes in the
  ``external_files`` subdirectories and the ``external_files_manifest_*`` files
  are computed differently. They are no longer deterministic between two export
  operations.

Fixed
+++++

The ``documents_manifest.txt`` contained duplicate entries.

v0.1.4 (2020-04-20)
-------------------

Fixed
+++++

* Allow directory given to :func:`~databroker_pack.unpack` to be relative.
* Fix bug that made :func:`~databroker_pack.unpack` unusable on Windows.

v0.1.3 (2020-04-06)
-------------------

Fixed
+++++

* ``databroker-pack`` accepts a relative path as the target ``directory``
  parameter

Changed
+++++++

* The (optional) copying invoked by ``databroker-pack ... --copy-external``
  (commandline interface) and :func:`~databroker_pack.copy_external_files`
  (Python interface) now uses :func:`shutil.copyfile` instead of
  :func:`shutil.copy2`. This requires fewer permissions on the directory
  containing the file of interest.

v0.1.2 (2020-04-06)
-------------------

Fixed
+++++

* Improved error messages.
* Optionally tolerate failures during file copying.

v0.1.1 (2020-04-03)
-------------------

Fixed a critical packaging issue that made the CLI unusuable unless run from
the root directory of the repository.

v0.1.0 (2020-04-03)
-------------------

Initial release
