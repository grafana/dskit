local image = 'golang:1.18.4';

local pipeline = {
  new(name):: {
    kind: 'pipeline',
    name: name,
  },
};

local step = {
  make(target, commands=[]):: {
    name: 'make-%s' % target,
    image: image,
    commands: commands + ['make %s' % target],
  },
};

[
  pipeline.new('validate-pr') {
    steps: [
      step.make('mod-check'),
      step.make('lint'),
      step.make('test'),
      step.make('check-protos', commands=[
        'apt-get update && apt-get -y install unzip',
        'go mod vendor',
      ]),
    ],
  },
]
