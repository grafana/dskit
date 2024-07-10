package modules

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/services"
)

func mockInitFunc() (services.Service, error) { return services.NewIdleService(nil, nil), nil }

func mockInitFuncFail() (services.Service, error) { return nil, errors.New("Error") }

func TestDependencies(t *testing.T) {
	var testModules = map[string]module{
		"serviceA": {
			initFn: mockInitFunc,
		},

		"serviceB": {
			initFn: mockInitFunc,
		},

		"serviceC": {
			initFn: mockInitFunc,
		},

		"serviceD": {
			initFn: mockInitFuncFail,
		},
	}

	mm := NewManager(log.NewNopLogger())
	for name, mod := range testModules {
		mm.RegisterModule(name, mod.initFn)
	}
	assert.NoError(t, mm.AddDependency("serviceB", "serviceA"))
	assert.NoError(t, mm.AddDependency("serviceC", "serviceB"))
	assert.Equal(t, mm.modules["serviceB"].deps, []string{"serviceA"})

	invDeps := mm.inverseDependenciesForModule("serviceA")
	assert.Equal(t, []string{"serviceB", "serviceC"}, invDeps)

	// Test unknown module
	svc, err := mm.InitModuleServices("service_unknown")
	assert.Error(t, err, fmt.Errorf("unrecognised module name: service_unknown"))
	assert.Empty(t, svc)

	// Test init failure
	svc, err = mm.InitModuleServices("serviceD")
	assert.Error(t, err)
	assert.Empty(t, svc)

	// Test loading several modules
	svc, err = mm.InitModuleServices("serviceA", "serviceB")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(svc))
	assert.Equal(t, []string{"serviceB"}, getStopDependenciesForModule("serviceA", svc))
	assert.Equal(t, []string(nil), getStopDependenciesForModule("serviceB", svc))

	svc, err = mm.InitModuleServices("serviceC")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(svc))
	assert.Equal(t, []string{"serviceB", "serviceC"}, getStopDependenciesForModule("serviceA", svc))
	assert.Equal(t, []string{"serviceC"}, getStopDependenciesForModule("serviceB", svc))
	assert.Equal(t, []string(nil), getStopDependenciesForModule("serviceC", svc))

	// Test loading of the module second time - should produce the same set of services, but new instances.
	svc2, err := mm.InitModuleServices("serviceC")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(svc))
	assert.NotEqual(t, svc, svc2)
	assert.Equal(t, []string{"serviceB", "serviceC"}, getStopDependenciesForModule("serviceA", svc))
	assert.Equal(t, []string{"serviceC"}, getStopDependenciesForModule("serviceB", svc))
	assert.Equal(t, []string(nil), getStopDependenciesForModule("serviceC", svc))
}

func TestManaged_AddDependency_ShouldErrorOnCircularDependencies(t *testing.T) {
	var testModules = map[string]module{
		"serviceA": {
			initFn: mockInitFunc,
		},

		"serviceB": {
			initFn: mockInitFunc,
		},

		"serviceC": {
			initFn: mockInitFunc,
		},
	}

	mm := NewManager(log.NewNopLogger())
	for name, mod := range testModules {
		mm.RegisterModule(name, mod.initFn)
	}
	assert.NoError(t, mm.AddDependency("serviceA", "serviceB"))
	assert.NoError(t, mm.AddDependency("serviceB", "serviceC"))

	// Direct circular dependency.
	err := mm.AddDependency("serviceB", "serviceA")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular dependency")

	// Indirect circular dependency.
	err = mm.AddDependency("serviceC", "serviceA")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular dependency")
}

func TestManaged_AddDependency_ShouldErrorIfModuleDoesNotExist(t *testing.T) {
	var testModules = map[string]module{
		"serviceA": {
			initFn: mockInitFunc,
		},

		"serviceB": {
			initFn: mockInitFunc,
		},
	}

	mm := NewManager(log.NewNopLogger())
	for name, mod := range testModules {
		mm.RegisterModule(name, mod.initFn)
	}
	assert.NoError(t, mm.AddDependency("serviceA", "serviceB"))

	// Module does not exist.
	err := mm.AddDependency("serviceUnknown", "serviceA")
	assert.EqualError(t, err, "no such module: serviceUnknown")

	// Dependency does not exist.
	err = mm.AddDependency("serviceA", "serviceUnknown")
	assert.EqualError(t, err, "no such module: serviceUnknown")
}

func TestRegisterModuleDefaultsToUserVisible(t *testing.T) {
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule("module1", mockInitFunc)

	m := sut.modules["module1"]

	assert.NotNil(t, mockInitFunc, m.initFn, "initFn not assigned")
	assert.True(t, m.userVisible, "module should be user visible")
}

func TestFunctionalOptAtTheEndWins(t *testing.T) {
	userVisibleMod := func(option *module) {
		option.userVisible = true
	}
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule("mod1", mockInitFunc, UserInvisibleModule, userVisibleMod, UserInvisibleModule)

	m := sut.modules["mod1"]

	assert.NotNil(t, mockInitFunc, m.initFn, "initFn not assigned")
	assert.False(t, m.userVisible, "module should be internal")
}

func TestGetAllUserVisibleModulesNames(t *testing.T) {
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule("userVisible3", mockInitFunc)
	sut.RegisterModule("userVisible2", mockInitFunc)
	sut.RegisterModule("userVisible1", mockInitFunc)
	sut.RegisterModule("internal1", mockInitFunc, UserInvisibleModule)
	sut.RegisterModule("internal2", mockInitFunc, UserInvisibleModule)

	pm := sut.UserVisibleModuleNames()

	assert.Equal(t, []string{"userVisible1", "userVisible2", "userVisible3"}, pm, "module list contains wrong element and/or not sorted")
}

func TestGetAllUserVisibleModulesNamesHasNoDupWithDependency(t *testing.T) {
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule("userVisible1", mockInitFunc)
	sut.RegisterModule("userVisible2", mockInitFunc)
	sut.RegisterModule("userVisible3", mockInitFunc)

	assert.NoError(t, sut.AddDependency("userVisible1", "userVisible2", "userVisible3"))

	pm := sut.UserVisibleModuleNames()

	// make sure we don't include any module twice because there is a dependency
	assert.Equal(t, []string{"userVisible1", "userVisible2", "userVisible3"}, pm, "module list contains wrong elements and/or not sorted")
}

func TestGetEmptyListWhenThereIsNoUserVisibleModule(t *testing.T) {
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule("internal1", mockInitFunc, UserInvisibleModule)
	sut.RegisterModule("internal2", mockInitFunc, UserInvisibleModule)
	sut.RegisterModule("internal3", mockInitFunc, UserInvisibleModule)
	sut.RegisterModule("internal4", mockInitFunc, UserInvisibleModule)

	pm := sut.UserVisibleModuleNames()

	assert.Len(t, pm, 0, "wrong result slice size")
}

func TestIsUserVisibleModule(t *testing.T) {
	userVisibleModName := "userVisible"
	invisibleModName := "invisible"
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule(userVisibleModName, mockInitFunc)
	sut.RegisterModule(invisibleModName, mockInitFunc, UserInvisibleModule)

	var result = sut.IsUserVisibleModule(userVisibleModName)
	assert.True(t, result, "module '%v' should be user visible", userVisibleModName)

	result = sut.IsTargetableModule(userVisibleModName)
	assert.True(t, result, "module '%v' should be targetable", userVisibleModName)

	result = sut.IsUserVisibleModule(invisibleModName)
	assert.False(t, result, "module '%v' should be invisible", invisibleModName)

	result = sut.IsTargetableModule(invisibleModName)
	assert.False(t, result, "module '%v' should not be targetable", invisibleModName)

	result = sut.IsUserVisibleModule("ghost")
	assert.False(t, result, "expects result be false when module does not exist")
}

func TestIsTargetableModule(t *testing.T) {
	defaultModName := "userVisible"
	invisibleModName := "invisible"
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule(defaultModName, mockInitFunc)
	sut.RegisterModule(invisibleModName, mockInitFunc, UserInvisibleTargetableModule)

	var result = sut.IsUserVisibleModule(defaultModName)
	assert.True(t, result, "module '%v' should be user visible", defaultModName)

	result = sut.IsTargetableModule(defaultModName)
	assert.True(t, result, "module '%v' should be targetable", defaultModName)

	result = sut.IsUserVisibleModule(invisibleModName)
	assert.False(t, result, "module '%v' should be invisible", invisibleModName)

	result = sut.IsTargetableModule(invisibleModName)
	assert.True(t, result, "module '%v' should be targetable", invisibleModName)

	result = sut.IsTargetableModule("ghost")
	assert.False(t, result, "expects result be false when module does not exist")
}

func TestIsModuleRegistered(t *testing.T) {
	successModule := "successModule"
	failureModule := "failureModule"

	m := NewManager(log.NewNopLogger())
	m.RegisterModule(successModule, mockInitFunc)

	var result = m.IsModuleRegistered(successModule)
	assert.True(t, result, "module '%v' should be registered", successModule)

	result = m.IsModuleRegistered(failureModule)
	assert.False(t, result, "module '%v' should NOT be registered", failureModule)
}

func TestManager_DependenciesForModule(t *testing.T) {
	m := NewManager(log.NewNopLogger())
	m.RegisterModule("test", nil)
	m.RegisterModule("dep1", nil)
	m.RegisterModule("dep2", nil)
	m.RegisterModule("dep3", nil)

	require.NoError(t, m.AddDependency("test", "dep2", "dep1"))
	require.NoError(t, m.AddDependency("dep1", "dep2"))
	require.NoError(t, m.AddDependency("dep2", "dep3"))

	deps := m.DependenciesForModule("test")
	assert.Equal(t, []string{"dep1", "dep2", "dep3"}, deps)
}

func TestManager_inverseDependenciesForModule(t *testing.T) {
	m := NewManager(log.NewNopLogger())
	m.RegisterModule("test", nil)
	m.RegisterModule("dep1", nil)
	m.RegisterModule("dep2", nil)
	m.RegisterModule("dep3", nil)

	require.NoError(t, m.AddDependency("test", "dep2", "dep1"))
	require.NoError(t, m.AddDependency("dep1", "dep2"))
	require.NoError(t, m.AddDependency("dep2", "dep3"))

	invDeps := m.inverseDependenciesForModule("test")
	assert.Equal(t, []string(nil), invDeps)

	invDeps = m.inverseDependenciesForModule("dep1")
	assert.Equal(t, []string{"test"}, invDeps)

	invDeps = m.inverseDependenciesForModule("dep2")
	assert.Equal(t, []string{"dep1", "test"}, invDeps)

	invDeps = m.inverseDependenciesForModule("dep3")
	assert.Equal(t, []string{"dep1", "dep2", "test"}, invDeps)
}

func TestModuleWaitsForAllDependencies(t *testing.T) {
	var serviceA services.Service

	initA := func() (services.Service, error) {
		serviceA = services.NewIdleService(func(context.Context) error {
			// Slow-starting service. Delay is here to verify that service for C is not started before this service
			// has finished starting.
			time.Sleep(1 * time.Second)
			return nil
		}, nil)

		return serviceA, nil
	}

	initC := func() (services.Service, error) {
		return services.NewIdleService(func(context.Context) error {
			// At this point, serviceA should be Running, because "C" depends (indirectly) on "A".
			if s := serviceA.State(); s != services.Running {
				return fmt.Errorf("serviceA has invalid state: %v", s)
			}
			return nil
		}, nil), nil
	}

	m := NewManager(log.NewNopLogger())
	m.RegisterModule("A", initA)
	m.RegisterModule("B", nil)
	m.RegisterModule("C", initC)

	// C -> B -> A. Even though B has no service, C must still wait for service A to start, before C can start.
	require.NoError(t, m.AddDependency("B", "A"))
	require.NoError(t, m.AddDependency("C", "B"))

	servsMap, err := m.InitModuleServices("C")
	require.NoError(t, err)

	// Build service manager from services, and start it.
	servs := []services.Service(nil)
	for _, s := range servsMap {
		servs = append(servs, s)
	}

	servManager, err := services.NewManager(servs...)
	require.NoError(t, err)
	assert.NoError(t, services.StartManagerAndAwaitHealthy(context.Background(), servManager))
	assert.NoError(t, services.StopManagerAndAwaitStopped(context.Background(), servManager))
}

func TestModuleService_InterruptedFastStartup(t *testing.T) {
	finishStarting := make(chan struct{})
	subserviceStarted := make(chan struct{})
	subserviceStopped := false

	subService := services.NewBasicService(func(_ context.Context) error {
		// We want to control the execution of this function via the test code,
		// so ignore the passed context because it will be cancelled shortly after entering this function.
		close(subserviceStarted)
		<-finishStarting
		return nil
	}, func(serviceContext context.Context) error {
		<-serviceContext.Done()
		return nil
	}, func(error) error {
		subserviceStopped = true
		return nil
	})

	noDepsFunc := func(string) map[string]services.Service { return nil }
	moduleSvc := NewModuleService("A", log.NewNopLogger(), subService, noDepsFunc, noDepsFunc)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-subserviceStarted
		cancel()
		close(finishStarting)
	}()

	// Start service using context that's going to be cancelled
	require.NoError(t, moduleSvc.StartAsync(ctx))
	// We get context cancelled error from service context cancellation.
	err := moduleSvc.AwaitRunning(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "starting module A")

	require.True(t, subserviceStopped)
	require.Equal(t, services.Failed, moduleSvc.State())
	require.Equal(t, services.Terminated, subService.State())
}

func TestModuleService_InterruptedSlowStartup(t *testing.T) {
	subserviceStarted := make(chan struct{})
	subserviceStopped := false

	subService := services.NewBasicService(func(serviceContext context.Context) error {
		close(subserviceStarted)
		// Prolong the startup until we have to stop.
		<-serviceContext.Done()
		return nil
	}, func(context.Context) error {
		require.Fail(t, "did not expect to enter running state; service should have been canceled while in starting Fn and go directly to stoppingFn")
		return nil
	}, func(error) error {
		subserviceStopped = true
		return nil
	})

	noDepsFunc := func(string) map[string]services.Service { return nil }
	moduleSvc := NewModuleService("A", log.NewNopLogger(), subService, noDepsFunc, noDepsFunc)

	// Don't wait for startup because it will be very slow.
	require.NoError(t, moduleSvc.StartAsync(context.Background()))

	<-subserviceStarted

	// We get context.Canceled from moduleService.start.
	err := services.StopAndAwaitTerminated(context.Background(), moduleSvc)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "starting module A")

	assert.True(t, subserviceStopped)
	require.Equal(t, services.Failed, moduleSvc.State())
	require.Equal(t, services.Terminated, subService.State())
}

func getStopDependenciesForModule(module string, services map[string]services.Service) []string {
	var deps []string
	for name := range services[module].(delegatedNamedService).Service.(*moduleService).stopDeps(module) {
		deps = append(deps, name)
	}

	sort.Strings(deps)
	return deps
}
