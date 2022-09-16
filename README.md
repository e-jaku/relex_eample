# fullstack-engineer assignment

# What to do

1. Create a new repository and push the original assignment to it
2. Create a pull request in your new repo with your solution, write a short description of your solution and flag for any constraints and/or trade-offs in your code. Treat it as you would like to treat any PR at work
3. Ask your contact person for reviewers to assign to your PR. Hopefully you'll exchange some comments & feedback before having a follow up discussion in person or online.

# Assignment

The task is to construct a HTTP server, written in go, that listens on `http://localhost:8080`,
takes in a hierarchy of items in a flat structure (`CSV`), and returns it as a
nested hierarchical structure (`JSON`).

## Running the tests

The tests are defined in `io_test.go` and can be run with `go test`.

Note that the code can be written in any language of choice (e.g. Python, Java, Scala, Go etc).

## The task

The input payload has the following schema:

| Column    | Type   | Required                                |
| --------- | ------ | --------------------------------------- |
| `level_1` | String | Yes                                     |
| `level_2` | String | Yes if `level_3` is given, otherwise no |
| `level_3` | String | No                                      |
| `item_id` | String | Yes                                     |

An example input:

```csv
level_1,level_2,item_id
category_1,category_2,item_1
category_1,category_3,item_2
```

The task is to respond with the following schema:

```json
{
  "children": {
    "$id": {
      "children": {
        "$item_id": {
          "item": true
        }
      }
    }
  }
}
```

For example:

```json
{
  "children": {
    "category_1": {
      "children": {
        "category_2": {
          "children": {
            "item_1": {
              "item": true
            }
          }
        },
        "category_3": {
          "children": {
            "item_2": {
              "item": true
            }
          }
        }
      }
    }
  }
}
```

## Special cases

Levels that contain empty strings should be interpreted as the end of that
hierarchy branch, for example:

```csv
level_1,level_2,item_id
category_1,,item_1
category_2,category_3,item_2
```

Corresponds to the following `JSON`:

```json
{
  "children": {
    "category_1": {
      "children": {
        "item_1": { "item": true }
      }
    },
    "category_2": {
      "children": {
        "category_3": {
          "children": {
            "item_2": { "item": true }
          }
        }
      }
    }
  }
}
```

Missing columns should be interpreted as empty for the remainder of that hierarchy path.

Inputs where level _n_ is empty but level _n+1_ is non-empty should return the
[http status code `Bad Request`][400] as these constitute an invalid structure.
The following, for example is an example of such an invalid payload:

```csv
level_1,level_2,item_id
,category_2,item_1
```

[400]: https://en.wikipedia.org/wiki/List_of_HTTP_status_codes#4xx_Client_errors
