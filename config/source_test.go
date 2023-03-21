// Copyright © 2022 Meroxa, Inc. & Yalantis
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestParseSource(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want Source
		err  error
	}{
		{
			name: "success_required_values",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      DefaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_has_one_key",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID"},
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      DefaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_has_two_keys",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id,name",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID", "NAME"},
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      DefaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_space_between_keys",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id, name",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				TrackingPrefix: DefaultTrackingPrefix,
				KeyColumns:     []string{"ID", "NAME"},
				Snapshot:       DefaultSnapshot,
				BatchSize:      DefaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_starts_with_space",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     " id,name",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID", "NAME"},
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      DefaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_ends_with_space",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id,name ",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID", "NAME"},
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      DefaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_space_between_keys_before_comma",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id ,name ",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID", "NAME"},
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      DefaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_two_spaces",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id,  name ",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID", "NAME"},
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      DefaultBatchSize,
			},
		},
		{
			name: "success_snapshot_is_false",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Snapshot:       "false",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				Snapshot:       false,
				TrackingPrefix: DefaultTrackingPrefix,
				BatchSize:      DefaultBatchSize,
			},
		},
		{
			name: "success_custom_batchSize",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "100",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID"},
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      100,
			},
		},
		{
			name: "success_batchSize_max",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "100000",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID"},
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      100000,
			},
		},
		{
			name: "success_batchSize_min",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "1",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID"},
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      1,
			},
		},
		{
			name: "success_custom_columns",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				Columns:        "id, name,age",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID"},
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      DefaultBatchSize,
				Columns:        []string{"ID", "NAME", "AGE"},
			},
		},
		{
			name: "success_default_tracking_prefix",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "ID",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				TrackingPrefix: DefaultTrackingPrefix,
				Snapshot:       DefaultSnapshot,
				BatchSize:      DefaultBatchSize,
			},
			err: nil,
		},
		{
			name: "success_custom_tracking_prefix",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "ID",
				TrackingPrefix: "MEROXA",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				TrackingPrefix: "MEROXA",
				Snapshot:       DefaultSnapshot,
				BatchSize:      DefaultBatchSize,
			},
			err: nil,
		},
		{
			name: "failure_required_orderingColumn",
			in: map[string]string{
				URL:        testURL,
				Table:      testTable,
				KeyColumns: "id",
				Columns:    "id,name,age",
			},
			err: errors.New(`"orderingColumn" value must be set`),
		},
		{
			name: "failure_invalid_snapshot",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Snapshot:       "test",
			},
			err: errors.New(`parse "snapshot": strconv.ParseBool: parsing "test": invalid syntax`),
		},
		{
			name: "failure_invalid_batchSize",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "a",
			},
			err: errors.New(`parse BatchSize: strconv.Atoi: parsing "a": invalid syntax`),
		},
		{
			name: "failure_missed_orderingColumn_in_columns",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "name",
				Columns:        "name,age",
			},
			err: fmt.Errorf("columns must include %q", OrderingColumn),
		},
		{
			name: "failure_missed_keyColumn_in_columns",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "name",
				Columns:        "id,age",
			},
			err: fmt.Errorf("columns must include all %q", KeyColumns),
		},
		{
			name: "failure_keyColumn_is_too_big",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns: "CREZSK8VR1LM5F0RZ5NA7FGJ0CNNVTTFNZDJKCWD8KKCU7UKZAW0NRNCZRQNM4EIAMEG0K7BGV2GX8UTDL" +
					"RLM8HGNFEYSSEXAL7V8GVFKWU8PQABQ1FH6LHEVFVGZ9XUF",
			},
			err: fmt.Errorf("%q is out of range", KeyColumns),
		},
		{
			name: "failure_batchSize_is_too_big",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "100001",
			},
			err: fmt.Errorf("%q is out of range", BatchSize),
		},
		{
			name: "failure_batchSize_is_zero",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "0",
			},
			err: fmt.Errorf("%q is out of range", BatchSize),
		},
		{
			name: "failure_batchSize_is_negative",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "-1",
			},
			err: fmt.Errorf("%q is out of range", BatchSize),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSource(tt.in)
			if err != nil {
				if tt.err == nil {
					t.Errorf("unexpected error: %s", err.Error())

					return
				}

				if err.Error() != tt.err.Error() {
					t.Errorf("unexpected error, got: %s, want: %s", err.Error(), tt.err.Error())

					return
				}

				return
			}

			// removing to simplify these tests
			// we're focusing on these in TestParseSource_HelperObjects_*
			got.SnapshotTable = ""
			got.TrackingTable = ""
			got.Trigger = ""
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got: %v, want: %v", got, tt.want)
			}
		})
	}
}

func TestParseSource_HelperObjects_AllSpecified(t *testing.T) {
	is := is.New(t)

	cfgMap := map[string]string{
		URL:            "test_url",
		Table:          "test_table",
		OrderingColumn: "test_column",

		SnapshotTable: "table_1",
		TrackingTable: "table_2",
		Trigger:       "trigger_3",
	}
	underTest, err := ParseSource(cfgMap)
	is.NoErr(err)
	is.Equal(strings.ToUpper(cfgMap[SnapshotTable]), underTest.SnapshotTable)
	is.Equal(strings.ToUpper(cfgMap[TrackingTable]), underTest.TrackingTable)
	is.Equal(strings.ToUpper(cfgMap[Trigger]), underTest.Trigger)
}

func TestParseSource_HelperObjects_PartiallySpecified(t *testing.T) {
	is := is.New(t)

	cfgMap := map[string]string{
		URL:            "test_url",
		Table:          "test_table",
		OrderingColumn: "test_column",

		SnapshotTable: "table_1",
		TrackingTable: "table_2",
	}
	underTest, err := ParseSource(cfgMap)
	is.NoErr(err)
	is.Equal(strings.ToUpper(cfgMap[SnapshotTable]), underTest.SnapshotTable)
	is.Equal(strings.ToUpper(cfgMap[TrackingTable]), underTest.TrackingTable)
	checkHelperObject(is, underTest.Trigger, DefaultTrackingPrefix)
}

func TestParseSource_HelperObjects_Prefix(t *testing.T) {
	testCases := []struct {
		name       string
		in         map[string]string
		wantPrefix string
		wantErr    error
	}{
		{
			name: "default prefix",
			in: map[string]string{
				URL:            "test_url",
				Table:          "test_table",
				OrderingColumn: "test_column",
			},
			wantPrefix: DefaultTrackingPrefix,
		},
		{
			name: "custom prefix",
			in: map[string]string{
				URL:            "test_url",
				Table:          "test_table",
				OrderingColumn: "test_column",
				TrackingPrefix: "custom_",
			},
			wantPrefix: "CUSTOM_",
		},
		{
			name: "custom prefix too long",
			in: map[string]string{
				URL:            "test_url",
				Table:          "test_table",
				OrderingColumn: "test_column",
				TrackingPrefix: "prefix_prefix_prefix_prefix_prefix_prefix_prefix_prefix_prefix_prefix_prefix_prefix_" +
					"prefix_prefix_prefix_prefix_prefix_prefix_prefix_prefix_prefix_",
			},
			wantPrefix: "",
			// includes error messages for the helper objects
			// as they are not generated at all
			wantErr: errors.New(`"trackingPrefix" is out of range; "" is out of range; "" is out of range; "" is out of range`),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			underTest, err := ParseSource(tc.in)
			if tc.wantErr != nil {
				if err == nil {
					t.Errorf("error expected, but got none: %s", tc.wantErr.Error())

					return
				}
				if err.Error() != tc.wantErr.Error() {
					t.Errorf("unexpected error, got: %s, want: %s", err.Error(), tc.wantErr.Error())

					return
				}

				return
			}

			is.NoErr(err)
			is.Equal(tc.wantPrefix, underTest.TrackingPrefix)
			checkHelperObject(is, underTest.SnapshotTable, tc.wantPrefix)
			checkHelperObject(is, underTest.TrackingTable, tc.wantPrefix)
			checkHelperObject(is, underTest.Trigger, tc.wantPrefix)
		})
	}
}

func checkHelperObject(is *is.I, s string, prefix string) {
	is.True(strings.HasPrefix(s, prefix))
	is.True(len(s) < 30)
}
