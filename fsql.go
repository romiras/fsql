package fsql

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/romiras/fsql/parser"
	"github.com/romiras/fsql/query"
)

// Run parses the input and executes the resultant query.
func Run(input string) error {
	q, err := parser.Run(input)
	if err != nil {
		return err
	}

	// Find length of the longest name to normalize name output.
	var max = 0
	var results = make(chan map[string]interface{})
	errChan := make(chan error)

	go func() {
		err = q.Execute(
			func(path string, info os.FileInfo, result map[string]interface{}) {
				results <- result
				if !q.HasAttribute("name") {
					return
				}
				if s, ok := result["name"].(string); ok && len(s) > max {
					max = len(s)
				}
			},
		)
		if err != nil {
			errChan <- err
		}
		close(results)
		close(errChan)
	}()

	printResults(q, results, os.Stdout)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func printResults(q *query.Query, results chan map[string]interface{}, w io.Writer) {
	for result := range results {
		var buf bytes.Buffer
		for j, attribute := range q.Attributes {
			// If the current attribute is "name", pad the output string by `max`
			// spaces.
			format := "%v"
			if attribute == "name" {
				format = fmt.Sprintf("%%-%ds", len(result[attribute].(string))) // 'max' not used
			}
			buf.WriteString(fmt.Sprintf(format, result[attribute]))
			if j != len(q.Attributes)-1 {
				buf.WriteString("\t")
			}
		}
		fmt.Fprintf(w, "%s\n", buf.String())
	}
}
