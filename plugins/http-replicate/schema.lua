return {
--  no_consumer = true,
  fields = {
    say_hello = { type = "boolean", default = true },
    http_endpoint = { required = true, type = "url" },
    timeout = { default = 60000, type = "number" },
    keepalive = { default = 60000, type = "number" }
  }
}