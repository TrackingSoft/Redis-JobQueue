cover -delete

#find t -name \*.t -print -exec perl -MDevel::Cover {} \;

perl -MDevel::Cover example.pl

perl -MDevel::Cover t/00_compile.t
perl -MDevel::Cover t/95_critic.t
perl -MDevel::Cover t/96_kwalitee.t
perl -MDevel::Cover t/97_distribution.t
perl -MDevel::Cover t/98_pod.t
perl -MDevel::Cover t/99_pod_coverage.t

perl -MDevel::Cover t/00_Job/00_compile.t
perl -MDevel::Cover t/00_Job/01_new.t
perl -MDevel::Cover t/00_Job/02_accessor.t
perl -MDevel::Cover t/00_Job/03_variability.t
perl -MDevel::Cover t/00_Job/04_job_attributes.t
perl -MDevel::Cover t/00_Job/98_pod.t
perl -MDevel::Cover t/00_Job/99_pod_coverage.t

perl -MDevel::Cover t/01_JobQueue/00_compile.t
perl -MDevel::Cover t/01_JobQueue/01_new.t
perl -MDevel::Cover t/01_JobQueue/02_add_job.t
perl -MDevel::Cover t/01_JobQueue/03_check_job_status.t
perl -MDevel::Cover t/01_JobQueue/04_load_job.t
perl -MDevel::Cover t/01_JobQueue/05_get_next_job.t
perl -MDevel::Cover t/01_JobQueue/06_update_job.t
perl -MDevel::Cover t/01_JobQueue/07_delete_job.t
perl -MDevel::Cover t/01_JobQueue/08_get_jobs.t
perl -MDevel::Cover t/01_JobQueue/09_quit.t
perl -MDevel::Cover t/01_JobQueue/98_pod.t
perl -MDevel::Cover t/01_JobQueue/99_pod_coverage.t

cover


