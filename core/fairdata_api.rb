# frozen_string_literal: true

require 'net/http'
require 'json'
require 'uri'

# Shared FairData (Core Data Cloud) REST API client used by per-project setup
# scripts (e.g. scripts/<project>/setup_project.rb).
#
# Staging and production run Clerk auth (VITE_AUTH_PROVIDER=clerk). Non-browser
# clients must send `User-Agent: node` (or `Server: Netlify`) so the connector's
# is_clerk? returns false and routes to JWT/password auth instead of expecting a
# Clerk session cookie — required on EVERY request, including /auth/login.
#
# Ruby 2.6 compatible (no endless methods / pattern matching) so it can run
# under system Ruby without bundler.
module FairDataApi
  USER_AGENT = 'node'

  class Client
    attr_reader :base_url

    def initialize(base_url)
      @base_url = base_url
      @token = nil
    end

    # POST /auth/login -> stores and returns the JWT.
    def login(email, password)
      result = request(:post, '/auth/login', { email: email, password: password }, nil)
      @token = result['token']
      raise "Login failed: no token in response" if @token.nil? || @token.empty?
      @token
    end

    def get(path)                 ; request(:get, path) ; end
    def post(path, body = nil)    ; request(:post, path, body) ; end
    def put(path, body = nil)     ; request(:put, path, body) ; end
    def patch(path, body = nil)   ; request(:patch, path, body) ; end
    def delete(path)              ; request(:delete, path) ; end

    private

    def request(method, path, body = nil, token = :use_session)
      uri = URI("#{@base_url}#{path}")
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = true

      req = case method
            when :get    then Net::HTTP::Get.new(uri)
            when :post   then Net::HTTP::Post.new(uri)
            when :put    then Net::HTTP::Put.new(uri)
            when :patch  then Net::HTTP::Patch.new(uri)
            when :delete then Net::HTTP::Delete.new(uri)
            else raise ArgumentError, "unsupported method #{method}"
            end

      req['Content-Type'] = 'application/json'
      req['Accept'] = 'application/json'
      req['User-Agent'] = USER_AGENT

      effective_token = token == :use_session ? @token : token
      req['Authorization'] = "Bearer #{effective_token}" if effective_token

      req.body = body.to_json if body

      res = http.request(req)
      unless res.code.to_i.between?(200, 299)
        raise "API #{method.to_s.upcase} #{path} returned #{res.code}: #{res.body && res.body.slice(0, 500)}"
      end

      begin
        JSON.parse(res.body)
      rescue StandardError
        {}
      end
    end
  end

  # The API doesn't return UDF labels, so match created UDFs to their code
  # definitions by `order` (which we control at creation time). Returns
  # { env_key => uuid }. Each def is a Hash with an :env key.
  def self.map_udfs_by_order(udf_defs, api_udfs)
    sorted = (api_udfs || []).sort_by { |u| u['order'].to_i }
    map = {}
    udf_defs.each_with_index do |udf_def, i|
      map[udf_def[:env]] = sorted[i]['uuid'] if sorted[i]
    end
    map
  end
end
