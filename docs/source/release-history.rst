===============
Release History
===============

v0.1.3 (2020-04-06)
-------------------

Fixed
+++++

* ``databroker-pack`` accepts a relative path as the target ``directory``
  parameter

Changed
-------

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
