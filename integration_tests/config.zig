const myzql = @import("myzql");
const Config = myzql.config.Config;
const integration_test_options = @import("integration_test_options");

const db_user: [:0]const u8 = integration_test_options.db_user[0..integration_test_options.db_user.len :0];
const db_name: [:0]const u8 = integration_test_options.db_name[0..integration_test_options.db_name.len :0];

pub const test_config: Config = .{
    .username = db_user,
    .password = integration_test_options.db_password,
    .address = myzql.config.Address.localhost(integration_test_options.db_port),
};

pub const test_config_with_db: Config = .{
    .username = db_user,
    .password = integration_test_options.db_password,
    .database = db_name,
    .address = myzql.config.Address.localhost(integration_test_options.db_port),
};
