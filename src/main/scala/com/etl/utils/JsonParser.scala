package com.etl.utils

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object JsonParser {

  val json = parse("""{
    "spec_version": 1.0,
    "transforms": [
    {
      "operation": "slugify",
      "column": "RecordLocation"
    },
    { "operation": "f-to-c",
      "column": "Temperature"
    },
    { "operation": "hst-to-unix",
      "column": "RecordedTime"
    },
    { "operation": "hst-to-unix",
      "column": "TimeSunRise"
    },
    { "operation": "hst-to-unix",
      "column": "TimeSunSet"
    }
    ]
  }"""
  )

  case class Transforms(operation:String,column:String)
  case class JsonNested(spec_version:String, transforms:List[Transforms])

  implicit val formats = DefaultFormats

  json.extract[JsonNested]


}

