.. Packaging Scientific Python documentation master file, created by
   sphinx-quickstart on Thu Jun 28 12:35:56 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

databroker-pack
===============

The Pitch
---------

The promise of "Data Broker" is to let users interact with scientific data
the same way they now interact with music in modern software. We rarely handle
music files directly: we search for *songs* described by attributes like
*release date*, *album*, and *artist*. There are files underneath somewhere,
but we rarely need to think about them. Data Broker aims to do the same for
scientific data.

But, you cannot email the abstract concept of a "song" to a friend---you email
an MP3. Likewise, when data needs to be manually moved between filesystems or
networks or archived, we usually need to interact with it at the level of
files.

The utility ``databroker-pack`` boxes up Bluesky Runs as a directory of files
which can be archived or transferred to other systems. At their destination, a
user can point ``databroker`` at this directory of files and use it like any
other data store.

The utility ``databroker-unpack`` installs a configuration file that makes this
directory easily "discoverable" so the recipient can access it as
``databroker.catalog.SOME_CATALOG_NAME``. This step is optional.

The content of this "packed" directory is intended to be internal---only
accessed via ``databroker``---but it employs widely-supported formats that can
be read via other means if the need arises.

.. toctree::
   :maxdepth: 2

   installation
   usage
   reference
   release-history
   min_versions
