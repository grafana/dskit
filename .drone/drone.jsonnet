local supported_golang_versions = [
  '1.18.4',
  '1.19.3',
];

local images = {
  golang(version):: 'golang:%s' % version,
  default:: self.golang(supported_golang_versions[0]),
};

local pipeline = {
  new(name):: {
    kind: 'pipeline',
    name: name,
  },
};

local step = {
  make(target, commands=[]):: {
    name: 'make-%s' % target,
    image: images.default,
    commands: commands + ['make %s' % target],
  },

  test(golang_version):: {
    name: 'make-test (go %s)' % golang_version,
    image: images.golang(golang_version),
    commands: ['make test'],
  },
};

local test_steps = [step.test(v) for v in supported_golang_versions];

[
  pipeline.new('validate-pr') {
    steps:
      [
        step.make('mod-check'),
        step.make('lint'),
      ] +
      test_steps +
      [
        step.make(
          'check-protos',
          commands=[
            'apt-get update && apt-get -y install unzip',
            'go mod vendor',
          ]
        ),
      ],
  },
]
