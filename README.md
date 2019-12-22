# stable

[![GoDoc](https://godoc.org/github.com/krpn/stable?status.svg)](http://godoc.org/github.com/krpn/stable) [![Build Status](https://github.com/krpn/stable/workflows/Go/badge.svg)](https://github.com/krpn/stable/actions) [![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=krpn_stable&metric=alert_status)](https://sonarcloud.io/dashboard?id=krpn_stable) [![Coverage Status](https://sonarcloud.io/api/project_badges/measure?project=krpn_stable&metric=coverage)](https://sonarcloud.io/component_measures?id=krpn_stable&metric=coverage) [![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=krpn_stable&metric=sqale_index)](https://sonarcloud.io/component_measures?id=krpn_stable&metric=sqale_index) [![License](https://img.shields.io/github/license/krpn/stable.svg)](https://github.com/krpn/stable/blob/master/LICENSE)

* `STable` is a simple **s**tring **table** engine with basic `(C)RUD` methods
* All rows are stored as a `map[string]string`
* `Primary key` and `constraints` are supported
* `Triggers` are supported
* It is `safe` calling `STable` methods from `concurrently` running goroutines

## Installation

```
go get github.com/krpn/stable
```

## Example Usage

```go
package main

import (
	"fmt"
	"github.com/krpn/stable"
)

func main() {
	customers, err := stable.NewSTable(
		nil,                       // initial data
		"id",                      // primary key field
		[]string{"name", "phone"}, // requered fields
		[]string{"phone"},         // uniq fields
	)
	if err != nil {
		panic(err)
	}
	_, err = customers.Insert([]map[string]string{
		{"id": "1", "name": "Alex", "phone": "112233", "city": "New-York"},
		{"id": "2", "name": "John", "phone": "223344", "city": "New-York"},
		{"id": "3", "name": "Bill", "phone": "334455", "city": "London", "position": "JSON Senior Developer"},
	})
	if err != nil {
		panic(err)
	}
	row, err := customers.SelectAny(map[string]string{"city": "New-York"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", row) // map[city:New-York id:1 name:Alex phone:112233]
	_, err = customers.Delete(map[string]string{"id": "2"})
	if err != nil {
		panic(err)
	}
	rows, err := customers.Select(nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", rows) // [map[city:New-York id:1 name:Alex phone:112233] map[city:London id:3 name:Bill phone:334455 position:JSON Senior Developer]]
}

```

