CREATE TABLE events
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{
"type":"record","name":"Event",
"fields":[
  {
    "name":"headers",
    "type":{"type":"map","values":"string"}},{"name":"body","type":"string"
  }
]}');
