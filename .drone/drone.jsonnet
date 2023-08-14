local supported_golang_versions = [
  '1.19.3',
  '1.20.4',
  '1.21.0',
];

local images = {
  golang(version):: 'golang:%s' % version,
  default:: self.golang(supported_golang_versions[std.length(supported_golang_versions)-1]),
};

local pipeline = {
  new(name):: {
    kind: 'pipeline',
    name: name,
  },
};

local depends_on(step) = { depends_on+: [step] };

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
  } + depends_on('make-lint'),
};

local test_steps = [step.test(v) for v in supported_golang_versions];

[
  pipeline.new('validate-pr') {
    steps:
      [
        step.make('mod-check'),
        step.make('lint') + depends_on('make-mod-check'),
      ] +
      test_steps +
      [
        step.make(
          'check-protos',
          commands=[
            'apt-get update && apt-get -y install unzip',
            'go mod vendor',
          ]
        ) + depends_on('make-mod-check'),
      ],
  },
]
