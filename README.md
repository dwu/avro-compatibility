Note: This is a fork of https://github.com/ExpediaGroup/avro-compatibility with updated dependencies and a CLI for testing schema compatibility.

## CLI use and example result

```
usage: avro-compatibility
 -m,--mutual         Check for mutual compatibility.
                     Default: Check that reader can read output produced
                     by writer.
 -r,--reader <arg>   Reader schema for compatibility check
 -w,--writer <arg>   Writer schema for compatibility check
```

Example result:
```
Result:
Writer schema CANNOT be read with reader schema.

Reason(s):
- Type: READER_FIELD_MISSING_DEFAULT_VALUE
  Location: /fields/3
  Writer: {"type":"record","name":"User","namespace":"example.avro","fields":[{"name":"name","type":"string"},{"name":"favorite_number","type":["int","null"]},{"name":"favorite_color","type":["string","null"]}]}
  Reader: {"type":"record","name":"User","namespace":"example.avro","fields":[{"name":"name","type":"string"},{"name":"favorite_number","type":["int","null"]},{"name":"favorite_color","type":["string","null"]},{"name":"foo","type":"string"}]}
```

Please see the upstream repo's README for credits and prior art.

## Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html). See `NOTICE` for further information.

Copyright 2017 Expedia Inc.
