package db

type DatabaseConfig struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Uri      string `mapstructure:"uri"`
	Database string `mapstructure:"database_name"`
}
