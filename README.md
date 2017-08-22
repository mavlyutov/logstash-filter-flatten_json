# Logstash Plugin

This is a plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

## Documentation

This filter plugin will flatten json data into strings, i.e.
```json
{
    "key_1": "value_1",
    "key_2": 2,
    "key_3": {
        "nested_key_1": "nested_value_1",
        "nested_key_2": [1, 2, "a"]
    }
}
```

example configuration:
```
filter {
    flatten_json {
        source => "message"
    }
}
```

the filter will enrich 'event' with following key-value pairs:
```ascii
key_1: "value_1"
key_2: 2
key_3.nested_key_1: "nested_value_1"
key_3.nested_key_2.0: 1
key_3.nested_key_2.1: 2
key_3.nested_key_2.2: "a"
```

This is very convenient if you have stats data from mongodb or docker-stats and you want to further process it or send to graphite server.

Logstash provides infrastructure to automatically generate documentation for this plugin. We use the asciidoc format to write documentation so any comments in the source code will be first converted into asciidoc and then into html. All plugin documentation are placed under one [central location](http://www.elastic.co/guide/en/logstash/current/).

- For formatting code or config example, you can use the asciidoc `[source,ruby]` directive
- For more asciidoc formatting tips, see the excellent reference here https://github.com/elastic/docs#asciidoc-guide

## Need Help?

Need help? Try #logstash on freenode IRC or the https://discuss.elastic.co/c/logstash discussion forum.

## Installation from sources
```bash
wget https://github.com/mavlyutov/logstash-filter-flatten_json/releases/download/0.1.0/logstash-filter-flatten_json-0.1.0.gem
/usr/share/logstash/bin/logstash-plugin install --no-verify logstash-filter-flatten_json-0.1.0.gem
```

## Developing

### 1. Plugin Developement and Testing

#### Code
- To get started, you'll need JRuby with the Bundler gem installed.

- Create a new plugin or clone and existing from the GitHub [logstash-plugins](https://github.com/logstash-plugins) organization. We also provide [example plugins](https://github.com/logstash-plugins?query=example).

- Install dependencies
```sh
bundle install
```

#### Test

- Update your dependencies

```sh
bundle install
```

- Run tests

```sh
bundle exec rspec
```

### 2. Running your unpublished Plugin in Logstash

#### 2.1 Run in a local Logstash clone

- Edit Logstash `Gemfile` and add the local plugin path, for example:
```ruby
gem "logstash-filter-awesome", :path => "/your/local/logstash-filter-awesome"
```
- Install plugin
```sh
bin/logstash-plugin install --no-verify
```
- Run Logstash with your plugin
```sh
bin/logstash -e 'filter {awesome {}}'
```
At this point any modifications to the plugin code will be applied to this local Logstash setup. After modifying the plugin, simply rerun Logstash.

#### 2.2 Run in an installed Logstash

You can use the same **2.1** method to run your plugin in an installed Logstash by editing its `Gemfile` and pointing the `:path` to your local plugin development directory or you can build the gem and install it using:

- Build your plugin gem
```sh
gem build logstash-filter-awesome.gemspec
```
- Install the plugin from the Logstash home
```sh
bin/logstash-plugin install /your/local/plugin/logstash-filter-awesome.gem
```
- Start Logstash and proceed to test the plugin

## Contributing

All contributions are welcome: ideas, patches, documentation, bug reports, complaints, and even something you drew up on a napkin.

Programming is not a required skill. Whatever you've seen about open source and maintainers or community members  saying "send patches or die" - you will not see that here.

It is more important to the community that you are able to contribute.

For more information about contributing, see the [CONTRIBUTING](https://github.com/elastic/logstash/blob/master/CONTRIBUTING.md) file.
