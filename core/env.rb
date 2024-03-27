require 'dotenv'

class Env
  ENV_DEVELOPMENT = 'development'
  ENV_STAGING = 'staging'

  def initialize_env(directory, environment = nil)
    filepath = "#{directory}/.env.#{environment || ENV_DEVELOPMENT}"
    puts filepath
    Dotenv.parse filepath
  end
end