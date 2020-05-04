=====
Usage
=====

There is a Python interface, but most users will find the commandline tool
suitable for their needs.

Packing a Catalog
-----------------

For the command line tool ``databroker-pack`` you must provide:

* The name of the source catalog
* The name of the target directory
* Which Runs in the Catalog to pack: either ``--all``, a query such as a time
  window, or a list of ``--uids``.

The result is a directory, which you can optionally compress and transfer by
any convenient means.

Examples
++++++++

List the available options for ``CATALOG`` and exit.

.. code:: bash

   databroker-pack --list-catalogs
   <list of catalog names>

Export every Run in the Catalog into a self-contained directory with Documents
and any external files (e.g. large array data from detectors).

.. code:: bash

   databroker-pack CATALOG --all DIRECTORY --copy-external

Or, read the data from the external files and place it directly in the
documents. This may make data access slower and less flexible, but it removes
the requiment for the recipient to install any special I/O code to deal with
detector formats.

.. code:: bash

   databroker-pack CATALOG --all DIRECTORY --fill-external

Or, omit the external files and transfer them separately. The ``DIRECTORY``
will still contain text file *manifests* listing the locations of the external
files on the source system, suitable for feeding to tools like ``rsync`` or
``globus transfer --batch``. This is like the recommended approach for very
large transfers.

.. code:: bash

   databroker-pack CATALOG --all DIRECTORY

Export Runs from a range of time.

.. code:: bash

   databroker-pack CATALOG -q "TimeRange(since='2020')" DIRECTORY
   databroker-pack CATALOG -q "TimeRange(since='2020', until='2020-03-01)" DIRECTORY

Export Runs from a range of time with a certain plan_name.

.. code:: bash

   databroker-pack CATALOG -q "TimeRange(since='2020')" -q "{'plan_name': 'count'}" DIRECTORY

Export a specific Run by its scan_id

.. code:: bash

   databroker-pack CATALOG -q "{'scan_id': 126360}" DIRECTORY

Export specific Runs given by their Run Start UID (or the first several
characters) entered at the command prompt...

.. code:: bash

   databroker-pack CATALOG --uids -
   3c93c54e
   47587fa8
   ebad8c01
   <Ctrl D>

...or read from a file.

.. code:: bash

   databroker-pack CATALOG --uids uids_to_pack.txt

Unpacking a Packed Catalog
--------------------------

There are two ways to do this:

# ``inplace`` --- Run databroker on top of the files as they are. This is
  recommended only for smalls exports (tens of Runs) when a Mongo database is
  not available.
# ``mongo_normalized`` --- Copy the documents from the packed directory into
  MongoDB, and point databroker at MongoDB.

Option 1: Unpacking "in place"
++++++++++++++++++++++++++++++

Use ``databroker-unpack`` to make ``DIRECTORY`` automatically discoverable by
databroker. You must specify a ``NAME`` to give the catalog.

If the name already exists, the catalog will be updated to include content
from the pre-existing location(s) and the new one. If you want to ensure that
this catalog name is unique, prohibiting automatic merging, use the flag
``--no-merge``.

.. code:: bash

   databroker-unpack inplace DIRECTORY NAME

For example

.. code:: bash

   databroker-unpack inplace path/to/directory_from_pack my_data

It is important not to move the directory after you do this.

Option 2: Unpacking into MongoDB
++++++++++++++++++++++++++++++++

.. note::

   If you need to install MongoDB, we refer you to the
   `official guides <https://docs.mongodb.com/manual/installation/#mongodb-community-edition-installation-tutorials>`_
   for installing the MongoDB Community Edition.

Use ``databroker-unpack`` to copy the data from the documents (stored in the
pack directory as ``.msgpack`` or ``.jsonl`` files) into MongoDB. Any external
files (e.g. large detector images stored separately) will be left where they
are and must done be deleted or moved once ``databroker-unpack`` has been run.

If the name already exists, the catalog will be updated to include content
from the pre-existing location(s) and the new one. If you want to ensure that
this catalog name is unique, prohibiting automatic merging, use the flag
``--no-merge``.

.. code:: bash

   databroker-unpack mongo_normalized DIRECTORY NAME

For example

.. code:: bash

   databroker-unpack mongo_normalized path/to/directory_from_pack my_data

By default this look for an unauthenticated MongoDB running on localhost on
the standard port. A custom MongoDB URI amy be specified using the option
``--mongo-uri MONGO_URI``. See ``databroker-unpack --help`` for more
information.

Using an Unpacked Catalog
-------------------------

Then the newly "unpacked" catalog (e.g. ``my_data``) will show in

.. code:: bash

   databroker-pack --list-catalogs

and can be accessed like

.. code:: python

   >>> import databroker
   >>> db = databroker.catalog['my_data'].get()

This catalog, ``db``, contains the packed Runs, which can be accessed in the
usual way like ``db['<uid>']``, ``db[<scan_id>]``, ``db[-1]``, or fully
enumerated (unwise if the Catalog is huge) ``list(db)``.

Use Without Unpacking
---------------------

Alternatively, you can run databroker on top of a directory generated by
``databroker-pack`` without any unpacking step.

.. important::

   Currently, the following only works if the packed directory is in its
   original location. In a future release, it will also work if the directory
   has been moved or copied to a different location.

.. code:: python

   import intake
   catalog = intake.open_catalog('DIRECTORY/catalog.yml')

replacing ``DIRECTORY`` with the path to the directory generated by
``databroker-pack``. This will contain a catalog named ``'packed_catalog'``,
which you can open like so.

.. code:: python

   db = catalog["packed_catalog"].get()
