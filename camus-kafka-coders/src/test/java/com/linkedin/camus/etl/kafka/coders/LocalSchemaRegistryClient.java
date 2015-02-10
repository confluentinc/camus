/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.camus.etl.kafka.coders;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class LocalSchemaRegistryClient implements SchemaRegistryClient {

  private final Map<String, Map<Schema, Integer>> schemaCache;
  private final Map<Integer, Schema> idCache;
  private final Map<String, Map<Schema, Integer>> versionCache;
  private final AtomicInteger ids;

  public LocalSchemaRegistryClient() {
    schemaCache = new HashMap<String, Map<Schema, Integer>>();
    idCache = new HashMap<Integer, Schema>();
    versionCache = new HashMap<String, Map<Schema, Integer>>();
    ids = new AtomicInteger(0);
  }

  private int getIdFromRegistry(String subject, Schema schema) throws IOException {
    for (Map.Entry<Integer, Schema> entry: idCache.entrySet()) {
      if (entry.getValue().toString().equals(schema.toString())) {
        generateVersion(subject, schema);
        return entry.getKey();
      }
    }
    int id = ids.incrementAndGet();
    idCache.put(id, schema);
    generateVersion(subject, schema);
    return id;
  }

  private void generateVersion(String subject, Schema schema) {
    ArrayList<Integer> versions = getAllVersions(subject);
    Map<Schema, Integer> schemaVersionMap;
    int currentVersion;
    if (versions.isEmpty()) {
      schemaVersionMap = new HashMap<Schema, Integer>();
      currentVersion = 1;
    } else {
      schemaVersionMap = versionCache.get(subject);
      currentVersion = versions.get(versions.size() - 1) + 1;
    }
    schemaVersionMap.put(schema, currentVersion);
    versionCache.put(subject, schemaVersionMap);
  }

  private ArrayList<Integer> getAllVersions(String subject) {
    ArrayList<Integer> versions = new ArrayList<Integer>();
    if (versionCache.containsKey(subject)) {
      versions.addAll(versionCache.get(subject).values());
      Collections.sort(versions);
    }
    return versions;
  }

  private Schema getSchemaByIdFromRegistry(int id) throws IOException {
    if (idCache.containsKey(id)) {
      return idCache.get(id);
    } else {
      throw new IOException("Cannot get schema from schema registry!");
    }
  }

  private int getVersionFromRegistry(String subject, Schema schema)
      throws IOException, RestClientException{
    if (versionCache.containsKey(subject)) {
      return versionCache.get(subject).get(schema);
    } else {
      throw new IOException("Cannot get version from schema registry!");
    }
  }

  @Override
  public synchronized int register(String subject, Schema schema)
      throws IOException, RestClientException {
    Map<Schema, Integer> schemaIdMap;
    if (schemaCache.containsKey(subject)) {
      schemaIdMap = schemaCache.get(subject);
    } else {
      schemaIdMap = new IdentityHashMap<Schema, Integer>();
      schemaCache.put(subject, schemaIdMap);
    }

    if (schemaIdMap.containsKey(schema)) {
      return schemaIdMap.get(schema);
    } else {
      int id = getIdFromRegistry(subject, schema);
      schemaIdMap.put(schema, id);
      return id;
    }
  }

  @Override
  public synchronized Schema getByID(int id) throws IOException, RestClientException {
    if (idCache.containsKey(id)) {
      return idCache.get(id);
    } else {
      Schema schema = getSchemaByIdFromRegistry(id);
      idCache.put(id, schema);
      return schema;
    }
  }

  @Override
  public synchronized int getLatestVersion(String subject)
      throws IOException, RestClientException {
    ArrayList<Integer> versions = getAllVersions(subject);
    if (versions.isEmpty()) {
      throw new IOException("No schema registered under subject!");
    } else {
      return versions.get(versions.size() - 1);
    }
  }

  @Override
  public String getLatestSchema(String subject)
      throws IOException, RestClientException {
    int version = getLatestVersion(subject);
    Map<Schema, Integer> schemaVersionMap = versionCache.get(subject);
    for( Map.Entry<Schema, Integer> entry: schemaVersionMap.entrySet()) {
      if (entry.getValue() == version) {
        return entry.getKey().toString();
      }
    }
    return null;
  }

  @Override
  public synchronized int getVersion(String subject, Schema schema)
      throws IOException, RestClientException{
    Map<Schema, Integer> schemaVersionMap;
    if (versionCache.containsKey(subject)) {
      schemaVersionMap = versionCache.get(subject);
    } else {
      schemaVersionMap = new HashMap<Schema, Integer>();
    }

    if (schemaVersionMap.containsKey(schema)) {
      return schemaVersionMap.get(schema);
    }  else {
      int version = getVersionFromRegistry(subject, schema);
      schemaVersionMap.put(schema, version);
      return version;
    }
  }
}
