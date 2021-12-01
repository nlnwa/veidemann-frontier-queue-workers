--
-- Copyright 2021 National Library of Norway.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--       http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

local fromQueueKey = KEYS[1]
local toQueueKey = KEYS[2]
local currentTimeMillis = ARGV[1]

local res = redis.call('ZRANGEBYSCORE', fromQueueKey, 0, currentTimeMillis)
local moved = 0
for _, key in ipairs(res) do
    redis.call('ZREM', fromQueueKey, key)
    redis.call('RPUSH', toQueueKey, key)
    moved = moved + 1
end
return moved
