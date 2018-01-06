# Apache NiFi Processor for Idyl E3 Entity Extraction Engine

[![Build Status](https://travis-ci.org/mtnfog/idyl-e3-nifi-processor.png?branch=master)](https://travis-ci.org/mtnfog/idyl-e3-nifi-processor)

This project is a custom NiFi processor that provides integration with Apache NiFi and Idyl E3. With this processor you can utlize Idyl E3's entity extraction capabilities from within your NiFi pipelines. To use this processor, clone the project, build it, and copy the resulting `nar` file to NiFi's `lib` directory.

A prebuilt nar file can also be [downloaded](http://www.mtnfog.com/?p=3843).

This project includes as a dependency the [Idyl E3 client SDK for Java](https://github.com/mtnfog/idyl-e3-java-sdk).

The project can be built as:

`mvn clean install`

For more information see our [blog post](http://www.mtnfog.com/blog/apache-extraction-engine/) about the project. This project is licensed under the Apache Software License, version 2.0.
