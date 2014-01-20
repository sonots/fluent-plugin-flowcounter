# encoding: UTF-8
require_relative '../helper'
require 'benchmark'
Fluent::Test.setup

def create_driver(config, tag = 'foo.bar')
  Fluent::Test::OutputTestDriver.new(Fluent::FlowCounterOutput, tag).configure(config)
end

# setup
message = {'message' => "2013/01/13T07:02:11.124202 INFO GET /ping"}
time = Time.now.to_i
CONFIG = %[
  count_keys *
]

normal_driver = create_driver(CONFIG)
disable_count_driver = create_driver(CONFIG + %[enable_count false])
disable_bytes_driver = create_driver(CONFIG + %[enable_bytes false])
disable_rate_driver  = create_driver(CONFIG + %[enable_rate false])

# bench
n = 200000
Benchmark.bm(13) do |x|
  x.report("normal")  { normal_driver.run  { n.times { normal_driver.emit(message, time)  } } }
  x.report("disable_count") { disable_count_driver.run { n.times { disable_count_driver.emit(message, time) } } }
  x.report("disable_bytes") { disable_bytes_driver.run { n.times { disable_bytes_driver.emit(message, time) } } }
  x.report("disable_rate") { disable_rate_driver.run { n.times { disable_rate_driver.emit(message, time) } } }
end
#                    user     system      total        real
#normal          3.170000   0.010000   3.180000 (  3.714624)
#disable_count   2.990000   0.000000   2.990000 (  3.530552)
#disable_bytes   2.440000   0.010000   2.450000 (  2.976524)
#disable_rate    2.780000   0.000000   2.780000 (  3.298427)
