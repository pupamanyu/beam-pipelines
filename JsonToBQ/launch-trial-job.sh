#!/usr/bin/env bash
#
# Copyright (C) 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Script to Launch the pipeline for JsonToBQ
#
JOBJAR="<FULL PATH>/JsonToBQ-1.0-all.jar"
PROJECT="<GCP PROJECT>"
OUTPUTDATASET="<BQ DATASET>"
OUTPUTTABLE="<BQ MAIN TABLENAME>"
ERRORTABLE="<BQ ERROR TABLENAME>"
# INPUT FILES are assumed to be in location <input bucket>/backup/topic=<topic name>/year=<year>/month=<month>/day=<day>/hour=<hour>
INPUTBUCKET="<INPUT BUCKET NAME>"
TOPIC="<topic name>"
INPUTDIRECTORY="${INPUTBUCKET}/backup/topic=${TOPIC}"
STAGINGBUCKET="<Dataflow Staging Regional Bucket Name>"
STAGINGDIRECTORY="${STAGINGBUCKET}/staging"
# Comma Separated Prefixes in the form of yyyy-MM-dd
INPUTPREFIXES="<yyyy-MM-dd>,...."
BQSCHEMAFILE="${STAGINGBUCKET}/${OUTPUTTABLE}_bq_schema.json"
JSONSCHEMAFILE="${STAGINGBUCKET}/${OUTPUTTABLE}_json_schema.json"
INPUTSUFFIX=".gz"
TEMPDIRECTORY="${STAGINGBUCKET}/temp"
RUNNER="DataFlow"
JOBTAGPREFIX="bq-load"
DISKSIZEGB=256
WORKERMACHINETYPE="n1-standard-8"
#MAXWORKERS=600
SUBNETWORK="https://www.googleapis.com/compute/v1/projects/<host vpc project>/regions/<region name>/subnetworks/<subnetwork name>"
AUTOSCALINGALGO="THROUGHPUT_BASED"
OUTPUTPARTITIONCOLUMN="<OUTPUT PARTITION COLUMN NAME>"
ERRORPARTITIONCOLUMN="<ERROR PARTITION COLUMN NAME>"
TIMESTAMPCOLUMN="<TIMESTAMP COLUMN NAME>"
PARTITIONCOLUMNDATEFORMAT="yyyy-MM-dd"
# Comma Separated of PII Fields
SENSITIVEFIELDS="<field1>,<field2>,..."
GEOROOTFIELDNAME="<root field name for the geo object>"
# INCLUDEFIELDS appiles only for Fixed Schema Custom Data
INCLUDEFIELDS="<fieldN>,<fieldM>"
# Custom Data Type
# Universe of all possible custom data types
VALID_CUSTOM_DATATYPES="<datatypename1>,<datatypename2>"
# In case of defined customized data types
CUSTOMDATATYPE_FIELDSELECTOR="<fieldT>"
# In case of defined customized data types
CUSTOMDATATYPE_EXCLUDING_FIELDSELECTOR="<fieldR>"
# List of the available data types to search using the field selector
CUSTOMDATATYPE="<datatypename1>"
# EXCLUDEFIELDS applies only for Variable Schema Custom Data where data properties maybe loaded as Strings
#EXCLUDEFIELDS=""
SANITIZE_JSON="true"

java \
  -jar ${JOBJAR} \
  --outputDatasetName=${OUTPUTDATASET} \
  --outputTableName=${OUTPUTTABLE} \
  --errorTableName=${ERRORTABLE} \
  --outputTablePartitionColumn=${OUTPUTPARTITIONCOLUMN} \
  --errorTablePartitionColumn=${ERRORPARTITIONCOLUMN} \
  --timestampColumn=${TIMESTAMPCOLUMN} \
  --partitionColumnDateFormat=${PARTITIONCOLUMNDATEFORMAT} \
  --sensitiveFields=${SENSITIVEFIELDS} \
  --geoRootFieldName=${GEOROOTFIELDNAME} \
  --inputDirectory=${INPUTDIRECTORY} \
  --project=${PROJECT} \
  --subnetwork=${SUBNETWORK} \
  --diskSizeGb=${DISKSIZEGB} \
  --workerMachineType=${WORKERMACHINETYPE} \
  --autoscalingAlgorithm=${AUTOSCALINGALGO} \
  --inputPrefixes=${INPUTPREFIXES} \
  --BQSchemaFile=${BQSCHEMAFILE} \
  --JSONSchemaFile=${JSONSCHEMAFILE} \
  --inputFilenameSuffix=${INPUTSUFFIX} \
  --tempLocation=${TEMPDIRECTORY} \
  --stagingLocation=${STAGINGDIRECTORY} \
  --runner=${RUNNER} \
  --jobTagPrefix=${JOBTAGPREFIX} \
  --validCustomDataTypes=${VALID_CUSTOM_DATATYPES} \
  --customDataTypeFieldSelector=${CUSTOMDATATYPE_FIELDSELECTOR} \
  --customDataTypeExcludingFieldSelector=${CUSTOMDATATYPE_EXCLUDING_FIELDSELECTOR} \
  --customDataType=${CUSTOMDATATYPE} \
  --sanitizeJson=${SANITIZE_JSON} \
  --filterCustomFields=${INCLUDEFIELDS}
