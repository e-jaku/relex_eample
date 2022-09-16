package hierarchy

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type testDefinition struct {
	Description    string
	In             string
	InFilename     string
	WantStatusCode int
	Want           Node
	WantFilename   string
}

type Node struct {
	Item     bool
	Children map[string]Node
}

func Test(t *testing.T) {
	testServer := "http://localhost:8080"

	tests := []testDefinition{
		{
			Description:    "well-formed payload",
			WantStatusCode: http.StatusOK,
			In:             wellFormedPayload,
			Want: Node{
				Children: map[string]Node{
					"A": Node{
						Children: map[string]Node{
							"C": Node{
								Children: map[string]Node{
									"1": Node{Item: true},
									"2": Node{Item: true},
								},
							},
							"D": Node{
								Children: map[string]Node{
									"3": Node{Item: true},
									"4": Node{Item: true},
								},
							},
						},
					},
					"B": Node{
						Children: map[string]Node{
							"E": Node{
								Children: map[string]Node{
									"5": Node{Item: true},
									"6": Node{Item: true},
								},
							},
						},
					},
				},
			},
		},
		{
			Description:    "erroneous non-N/A at higher depth",
			WantStatusCode: http.StatusBadRequest,
			In:             illegalNaPayload,
		},
		{
			Description:    "test files read from disk",
			WantStatusCode: http.StatusOK,
			InFilename:     filepath.Join("testdata", "small_input.csv"),
			WantFilename:   filepath.Join("testdata", "small_output.json"),
		},
		{
			Description:    "large test",
			WantStatusCode: http.StatusOK,
			InFilename:     filepath.Join("testdata", "large_input.csv"),
			WantFilename:   filepath.Join("testdata", "large_output.json"),
		},
	}

	for _, test := range tests {
		t.Run(test.Description, func(t *testing.T) {
			reqBody, want, err := loadTest(test)
			if err != nil {
				t.Fatalf("test is broken: %s", err)
			}

			resp, err := http.Post(testServer, "text/csv", reqBody)
			if err != nil {
				t.Fatal("could not perform request: ", err)
			}

			if resp.StatusCode != test.WantStatusCode {
				t.Errorf("got status code (=%d), want %d", resp.StatusCode, test.WantStatusCode)
			}

			defer resp.Body.Close()

			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("could not read body: %s", err)
			}

			var got Node
			if err := json.Unmarshal(b, &got); err != nil && test.WantStatusCode != http.StatusBadRequest {
				t.Fatalf("could not unmarshal payload: %s", err)
			}

			if !reflect.DeepEqual(got, want) {
				t.Errorf("got %v, want %v", got, test.Want)
			}
		})
	}
}

const wellFormedPayload = `item_id,level_1,level_2
1,A,C
2,A,C
3,A,D
4,A,D
5,B,E
6,B,E
`

const illegalNaPayload = `item_id,level_1,level_2
1,A,C
2,A,C
3,,D
4,A,D
5,B,E
6,B,E
`

func loadTest(test testDefinition) (reqBody io.Reader, want Node, err error) {
	if test.InFilename != "" {
		reqBody, err = os.Open(test.InFilename)
		if err != nil {
			err = fmt.Errorf("could not read golden input file: %s", err)
			return
		}
	} else {
		reqBody = strings.NewReader(test.In)
	}

	if test.WantFilename != "" {
		b, readErr := ioutil.ReadFile(test.WantFilename)
		if readErr != nil {
			err = fmt.Errorf("could not read golden output file: %s", readErr)
			return
		}
		if err = json.Unmarshal(b, &want); err != nil {
			err = fmt.Errorf("could not unmarshal golden output file: %s", err)
			return
		}
	} else {
		want = test.Want
	}

	return
}
