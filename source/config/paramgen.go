// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package config

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	ConfigBatchSize      = "batchSize"
	ConfigColumns        = "columns"
	ConfigKeyColumns     = "keyColumns"
	ConfigOrderingColumn = "orderingColumn"
	ConfigSnapshot       = "snapshot"
	ConfigSnapshotTable  = "snapshotTable"
	ConfigTable          = "table"
	ConfigTrackingTable  = "trackingTable"
	ConfigTrigger        = "trigger"
	ConfigUrl            = "url"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		ConfigBatchSize: {
			Default:     "1000",
			Description: "BatchSize is a size of rows batch.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{
				config.ValidationGreaterThan{V: 0},
				config.ValidationLessThan{V: 100001},
			},
		},
		ConfigColumns: {
			Default:     "",
			Description: "Columns list of column names that should be included in each Record's payload.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigKeyColumns: {
			Default:     "",
			Description: "KeyColumns is the configuration of key column names, separated by commas.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigOrderingColumn: {
			Default:     "",
			Description: "OrderingColumn is a name of a column that the connector will use for ordering rows.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigSnapshot: {
			Default:     "true",
			Description: "Snapshot is the configuration that determines whether the connector\nwill take a snapshot of the entire table before starting cdc mode.",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
		ConfigSnapshotTable: {
			Default:     "",
			Description: "SnapshotTable is the snapshot table to be used.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigTable: {
			Default:     "",
			Description: "Table is the configuration of the table name.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigTrackingTable: {
			Default:     "",
			Description: "TrackingTable is the tracking table to be used in CDC.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigTrigger: {
			Default:     "",
			Description: "Trigger is the trigger to be used in CDC.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigUrl: {
			Default:     "",
			Description: "URL is the configuration of the connection string to connect to Oracle database.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}
