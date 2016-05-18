.. _camus_intro:

Camus
=====

.. warning::

   .. deprecated:: 3.0.0

      Camus is deprecated in Confluent Platform and may be removed in the next major version.
      We recommend :ref:`Kafka Connect<kafka_connect>` and the :ref:`HDFS connector<connect_hdfs>`
      to export data from Kafka to HDFS and Hive.

Camus is a simple MapReduce job developed by LinkedIn to load data from Kafka into HDFS.
It is capable of incrementally copying data from Kafka into HDFS such that
every run of the MapReduce job picks up where the previous run left off.
At LinkedIn, Camus is used to load billions of messages per day from Kafka into HDFS.
Confluent's version of Camus integrates with Confluent's Schema Registry which
ensures data compatibility when loading to HDFS as schemas are evolved. You can find the design
and architecture of Camus in the :ref:`design section<camus_design>`.

Key Features
------------
    #. Automatic topic discovery: When a Camus job starts, it automatically fetches available topics
       from Zookeeper and offsets from Kafka and filters topics.

    #. Avro schema management: Camus integrates with Confluent's Schema Registry to ensure
       compatibility as Avro schema evolves.

    #. Output partitioning: Camus automatically partitions the output based on the timestamp of each
       record.

    #. Fault tolerance: Camus saves previous Kafka ETL requests and topic partition offsets to HDFS
       to provide fault tolerance on Zookeeper and Kafka failures. It also uses temp work directory
       to ensure consistency between Kafka and HDFS.

    #. Customizability: Many components of Camus are customizable. Camus provides interfaces for
       customized implementations of message decoder, data writer, data partitioner and
       work allocator.

    #. Load balance: Camus evenly assigns data to MapReduce tasks based on the size of
       each topic partitions. Moreover, as Camus jobs use temp working directories, speculative execution
       can be effective for straggler migration.

    #. Low operation overhead: Camus offers configurations to balance contention between topics and to
       control the Camus job behavior in case of incompatible data. By default, Camus will not
       fail the MapReduce job in case of incompatible data.

Quickstart
----------

.. ifconfig:: platform_docs

   These instructions assume you have already installed Confluent Platform and that
   you have access to a Hadoop Cluster. For installation and deployment of a single node
   Hadoop in pseudo-distributed mode, see this
   `guide <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cdh_qs_cdh5_pseudo.html>`_.
   Finally, you should have :ref:`Kafka and the Schema Registry running<quickstart>` .

.. ifconfig:: not platform_docs

   These instructions assume you have already installed Confluent Platform and that
   you have access to a Hadoop Cluster. For installation and deployment of a single node
   Hadoop in pseudo-distributed mode, see this
   `guide <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cdh_qs_cdh5_pseudo.html>`_.
   Finally, you should have Kafka and the Schema Registry running.

The recommended way to run a Camus job is via a small wrapper script, ``bin/camus-run``. It
sets the environment variables and passes the arguments required to get all the jars deployed
correctly and ensures the Camus jars are given priority, which ensures compatibility across a
variety of Hadoop distributions.

.. sourcecode:: bash

   # Assuming that you have hadoop on your PATH, HADOOP_CONF_DIR is properly configured, and that
   # schema.registry.url points to the correct address.
   $ bin/camus-run -D schema.registry.url=http://localhost:8081 -P etc/camus/camus.properties

If you need more control over how the job is executed, see the `Deployment`_ section for more
details about required configuration.

Once the Camus job is successfully completed, a couple of Avro files are created under
the topic output directory in sub-directories for each topic and date partition.
One example of full filename is
``/user/username/topics/testAvro/hourly/2015/02/16/15/testAvro.1.0.10.11.1424127600000.avro``.
The filename is ``.`` separated format that embeds metadata as
``TopicName.BrokerId.PartitionId.NumberRecords.FinalOffset.UTC``.

You may use Hive or other tools to perform offline analysis on the ingested Avro files.


Installation
------------

.. ifconfig:: platform_docs

   See the :ref:`installation instructions<installation>` for the Confluent
   Platform. Before starting a Camus job you must have Hadoop, Kafka, and the
   Schema Registry running. The :ref:`Confluent Platform quickstart<quickstart>`
   explains how to start Kafka and the Schema Registry locally for testing.
   See this
   `guide <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cdh_qs_cdh5_pseudo.html>`_
   to setup a single Hadoop node in pseudo-distributed mode.

.. ifconfig:: not platform_docs

   You can download prebuilt versions of Camus as part of the
   `Confluent Platform <http://confluent.io/downloads/>`_. To install from
   source, follow the instructions in the `Development`_ section.
   Before starting a Camus job you must have Hadoop, Kafka, and the
   Schema Registry running. You can find instructions for Kafka and the Schema
   Registry in the `Schema Registry repository <http://github.com/confluentinc/schema-registry>`_.
   This `guide <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cdh_qs_cdh5_pseudo.html>`_.
   to setup a single Hadoop node in pseudo-distributed mode.


Deployment
----------

Camus can be run from the command line. You will need to set some configurations either by specifying a
properties file on the classpath using ``-p`` (filename), or an external properties file using ``-P``
(path to local file system, or to hdfs),
or from the command line using ``-D property=value``.
If the same property is set with multiple methods,
the order of precedence is command-line properties, external properties file and
classpath properties file. You can find a list of settings in :ref:`configuration section<camus_config>` .

The recommended deployment method is to use the ``camus-run`` script to initiate the MapReduce job:

.. sourcecode:: bash

   $ bin/camus-run -D <property=value> \
      -P <path to external properties file> \
      -p <path to properties file from classpath>

If you need more control you can run the job yourself, but will have to configure some parameters
and environment variables yourself. You may want to reuse the ``bin/camus-config`` script to generate the
configs without running the job. That script should be sourced into your script
and sets up four environment variables:

#. ``HADOOP_CLASSPATH`` is updated to include the jars for Camus and its dependencies
#. ``CAMUS_LIBJARS`` contains a comma separated list of the jars for Camus and
   its dependencies, suitable for use as the value of the ``-libjars`` option
#. ``CAMUS_JAR`` is set to the primary Camus jar, which is the jar file you
   should pass as the first argument to ``hadoop jar``
#. ``HADOOP_USER_CLASSPATH_FIRST`` is set to true to ensure versions of
   libraries included with the Hadoop distribution do not conflict with versions
   required by Camus

If you do not use the ``camus-config`` script, you will need to configure these
settings manually. If you install Camus via zip/tgz archive,
you can find Camus's jar files under ``share/java/camus/``. If you install Camus via rpm or deb,
the Camus's jar files under ``/usr/share/java/camus/``. After configuring the
appropriate settings, as listed here, you can run the job with a command like this:

.. sourcecode:: bash

   # Assuming:
   # 1. hadoop is on your PATH
   # 2. HADOOP_CLASSPATH includes all the Camus jars
   # 3. HADOOP_USER_CLASSPATH_FIRST=true
   # 4. CAMUS_LIBJARS contains the comma-separated list of all Camus jars
   $ cd camus
   $ hadoop jar confluent-camus-$VERSION.jar com.linkedin.camus.etl.kafka.CamusJob \
      -libjars $CAMUS_LIBJARS -D mapreduce.job.user.classpath.first=true \
      -D <property=value> \
      -P <path to external properties file> \
      -p <path to properties file from classpath>


For some Hadoop distributions, you may be able to remove some of these settings to simplify the
command. Specifically, the ``HADOOP_USER_CLASSPATH_FIRST`` environment
variable and the ``mapreduce.job.user.classpath.first`` setting are
only required when jars included on the classpath by the Hadoop distribution are
too old to satisfy Camus's requirements.

Development
-----------

To build a development version of Camus, you need to get development version of the
`Schema Registry <https://github.com/confluentinc/schema-registry>`_ and its dependencies
and install it into local Maven repository. Once the dependencies are installed, you can build
Confluent version of Camus as follows:

.. sourcecode:: bash

    $ git clone https://github.com/confluentinc/camus.git
    $ cd camus
    $ mvn clean package

Requirements
------------

- Hadoop: Camus works with both MRv1 and YARN. We recommend CDH 5.3.x or HDP 2.2.x.
- Kafka: 0.10.1.0-SNAPSHOT
- Schema Registry: Confluent Schema Registry 3.1.0-SNAPSHOT

Contribute
----------

.. warning::

   Because Camus is deprecated both in Confluent Platform and upstream, only critical bug fixes are accepted to these
   repositories.

- Upstream Repository: https://github.com/linkedin/camus
- Source Code: https://github.com/confluentinc/camus
- Issue Tracker: https://github.com/confluentinc/camus/issues

License
-------

The project is licensed under the Apache 2 license.
