//! Helpers for bundling the crate's SQL migrations with your own.

/// Extend an `sqlx::migrate!()` call with Job's migrations.
pub trait IncludeMigrations {
    /// Append the Job crate migrations to the current migrator.
    fn include_job_migrations(&mut self) -> &mut Self;
}

impl IncludeMigrations for sqlx::migrate::Migrator {
    fn include_job_migrations(&mut self) -> &mut Self {
        let mut new_migrations = self.migrations.to_vec();
        new_migrations.extend_from_slice(&sqlx::migrate!().migrations);

        self.migrations = std::borrow::Cow::Owned(new_migrations);

        self
    }
}
