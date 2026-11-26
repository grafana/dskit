package modules

import (
	"fmt"
	"slices"
	"sort"

	"github.com/go-kit/log"
	"github.com/pkg/errors"

	"github.com/grafana/dskit/services"
)

// module is the basic building block of the application
type module struct {
	// dependencies of this module
	deps []string

	// reversed dependencies of this module
	reversedDeps []string

	// initFn for this module (can return nil)
	initFn func() (services.Service, error)

	// is this module user visible
	userVisible bool

	// is the module allowed to be selected as a target
	targetable bool
}

// Manager is a component that initialises modules of the application
// in the right order of dependencies.
type Manager struct {
	modules map[string]*module
	logger  log.Logger
}

// UserInvisibleModule is an option for `RegisterModule` that marks module not visible to user. Modules are user visible by default.
func UserInvisibleModule(m *module) {
	m.userVisible = false

	// by default invisible modules should not be targetable.
	m.targetable = false
}

// UserInvisibleTargetableModule is an option for `RegisterModule` that marks module not visible to user, but still keeps it targetable.
// Modules are visible and targetable by default.
func UserInvisibleTargetableModule(m *module) {
	m.userVisible = false

	// ensure that the module is still targetable.
	m.targetable = true
}

// NewManager creates a new Manager
func NewManager(logger log.Logger) *Manager {
	return &Manager{
		modules: make(map[string]*module),
		logger:  logger,
	}
}

// RegisterModule registers a new module with name, init function, and options. Name must
// be unique to avoid overwriting modules. If initFn is nil, the module will not initialise.
// Modules are user visible by default.
func (m *Manager) RegisterModule(name string, initFn func() (services.Service, error), options ...func(option *module)) {
	if _, ok := m.modules[name]; ok {
		panic(fmt.Sprintf("module %s already registered", name))
	}
	m.modules[name] = &module{
		initFn:      initFn,
		userVisible: true,
		targetable:  true,
	}

	for _, o := range options {
		o(m.modules[name])
	}
}

// AddDependency adds a dependency from name(source) to dependsOn(targets)
// An error is returned if the source module name is not found
func (m *Manager) AddDependency(name string, dependsOn ...string) error {
	mod, ok := m.modules[name]
	if !ok {
		return fmt.Errorf("no such module: %s", name)
	}

	// Ensure it doesn't introduce any circular dependency.
	for _, newDep := range dependsOn {
		if newDep == name {
			return fmt.Errorf("module %s cannot depend on itself", name)
		}
		if _, ok := m.modules[newDep]; !ok {
			return fmt.Errorf("no such module: %s", newDep)
		}
	}
	for _, n := range m.inverseDependenciesForModule(name) {
		if slices.Contains(dependsOn, n) {
			return fmt.Errorf("found a circular dependency: %s depends on %s", n, name)
		}
	}

	mod.deps = append(mod.deps, dependsOn...)
	for _, dep := range dependsOn {
		m.modules[dep].reversedDeps = append(m.modules[dep].reversedDeps, name)
	}
	return nil
}

// InitModuleServices initialises given modules by initialising all their dependencies
// in the right order. Modules are wrapped in such a way that they start after their
// dependencies have been started and stop before their dependencies are stopped.
func (m *Manager) InitModuleServices(modules ...string) (map[string]services.Service, error) {
	servicesMap := map[string]services.Service{}
	initMap := map[string]bool{}

	for _, module := range modules {
		if err := m.initModule(module, initMap, servicesMap); err != nil {
			return nil, err
		}
	}

	return servicesMap, nil
}

func (m *Manager) initModule(name string, initMap map[string]bool, servicesMap map[string]services.Service) error {
	if _, ok := m.modules[name]; !ok {
		return fmt.Errorf("unrecognised module name: %s", name)
	}

	// initialize all of our dependencies first
	deps := m.orderedDeps(name)
	deps = append(deps, name) // lastly, initialize the requested module

	for _, n := range deps {
		// Skip already initialized modules
		if initMap[n] {
			continue
		}

		mod := m.modules[n]

		var serv services.Service

		if mod.initFn != nil {
			s, err := mod.initFn()
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("error initialising module: %s", n))
			}

			if s != nil {
				// We pass servicesMap, which isn't yet complete. By the time service starts,
				// it will be fully built, so there is no need for extra synchronization.
				serv = newModuleServiceWrapper(servicesMap, n, m.logger, s, m.DependenciesForModule(n), m.inverseDependenciesForModule(n))
			}
		}

		if serv != nil {
			servicesMap[n] = serv
		}

		initMap[n] = true
	}

	return nil
}

// UserVisibleModuleNames gets list of module names that are
// user visible. Returned list is sorted in increasing order.
func (m *Manager) UserVisibleModuleNames() []string {
	var result []string
	for key, val := range m.modules {
		if val.userVisible {
			result = append(result, key)
		}
	}

	sort.Strings(result)

	return result
}

// IsUserVisibleModule check if given module is public or not. Returns true
// if and only if the given module is registered and is public.
func (m *Manager) IsUserVisibleModule(mod string) bool {
	val, ok := m.modules[mod]

	if ok {
		return val.userVisible
	}

	return false
}

// IsTargetableModule check if given module is targetable or not. Returns true
// if and only if the given module is registered and is targetable.
func (m *Manager) IsTargetableModule(mod string) bool {
	val, ok := m.modules[mod]

	if ok {
		return val.targetable
	}

	return false
}

// IsModuleRegistered checks if the given module has been registered or not. Returns true
// if the module has previously been registered via a call to RegisterModule, false otherwise.
func (m *Manager) IsModuleRegistered(mod string) bool {
	_, ok := m.modules[mod]
	return ok
}

// orderedDeps gets a list of all dependencies ordered so that items are always after any of their dependencies.
func (m *Manager) orderedDeps(mod string) []string {
	return m.traverse(mod, func(m *module) []string { return m.deps })
}

// inverseDependenciesForModule returns the list of modules depending on the input module, sorted by name.
func (m *Manager) inverseDependenciesForModule(mod string) []string {
	result := m.traverse(mod, func(m *module) []string { return m.reversedDeps })

	sort.Strings(result)
	return result
}

// traverse walks the graph reachable from mod using getNeighbours, appending nodes in post-order,
// so that a node always appears after all of its neighbours. mod itself is never included in the result.
// The visited set keeps the complexity at O(V+E) and makes the traversal safe on graphs with cycles.
func (m *Manager) traverse(mod string, getNeighbours func(*module) []string) (result []string) {
	visited := map[string]struct{}{}

	var f func(string)
	f = func(n string) {
		if _, ok := visited[n]; ok {
			return
		}
		visited[n] = struct{}{}
		for _, d := range getNeighbours(m.modules[n]) {
			f(d)
		}
		if n != mod {
			result = append(result, n)
		}
	}

	f(mod)

	return
}

// DependenciesForModule returns transitive dependencies for given module, sorted by name.
func (m *Manager) DependenciesForModule(module string) []string {
	result := m.orderedDeps(module)
	sort.Strings(result)
	return result
}
