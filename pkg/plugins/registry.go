package plugins

import "encoding/json"

// FactoryFunc instantiates a plugin from its instance name, raw JSON parameters
// (as found in configuration), and a Handle to host-provided context. The
// signature mirrors EPP so plugin authoring is familiar across the projects.
type FactoryFunc func(name string, parameters json.RawMessage, handle Handle) (Plugin, error)

// Registry maps a plugin type to its FactoryFunc.
var Registry = map[string]FactoryFunc{}

// Register registers a plugin factory under the given type. It is intended to be
// called from plugin package init() functions. Registering the same type twice
// overwrites the previous registration.
func Register(pluginType string, factory FactoryFunc) {
	Registry[pluginType] = factory
}
