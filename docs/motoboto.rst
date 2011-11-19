.. motoboto documentation master file, created by
   sphinx-quickstart on Fri Nov 18 13:25:56 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to motoboto's documentation!
====================================

Contents:

.. toctree::
   :maxdepth: 2

An Introduction to motoboto's nimbus.io interface
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Just as you would use `boto`_ to access Amaonzon's Simple Storage Service,
you can use motoboto to access nimbus.io. 

Identity
--------
.. automodule:: motoboto.identity
    :members: identity_template, load_identity_from_environment,
        load_identity_from_file

Creating a Connection
---------------------

The first step in accessing nimbus.io is to create a connection to the service. 
There are two ways to do this in motoboto. The first is:::

    >>> from motoboto.s3_emulator import S3Emulator
    >>> conn = S3Emulator(identity)

At this point the variable conn will point to an S3Emulator object.

There is also a shortcut function in the motoboto package, called connect_s3 
that may provide a slightly easier means of creating a connection:::

    >>> import motoboto
    >>> conn = motoboto.connect_s3(identity)

S3 Emulator
-----------
.. autoclass:: motoboto.s3_emulator.S3Emulator
    :members:

Bucket
------
.. autoclass:: motoboto.s3.bucket.Bucket
    :members:

Key
---
.. autoclass:: motoboto.s3.key.Key
    :members:

Test
----
.. autoclass:: tests.test_s3_replacement.TestS3

.. _boto: http://boto.s3.amazonaws.com/s3_tut.html

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

