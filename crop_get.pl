#!/usr/bin/perl -w
# $Id: crop_get.pl 1989 2010-05-25 17:16:30Z wbilodeau $

use strict;
use DBI;
use Data::Dumper qw( Dumper );
use Storable;
use Fcntl qw( :flock );
use Storable qw( store retrieve );
use Digest::MD5::File qw( file_md5_hex );
use POSIX qw( nice );
use EP;
use Getopt::Std qw( getopts );
use Config::Auto;

#checi for a config file.
my $config;

# Make sure there is a config file
my %O;
getopts( 'c:', \%O );
if ( exists( $O{'c'} ) ) {
	$config    = Config::Auto::parse( $O{'c'}, format => "perl");
} else {
	die "No config file";
}

# Set log
log_set_dir( $config->{'LOG_DIR'} ); 
log_set_name( $config->{'LOG_FILE'} );
log_tie_stderr();

# Set PGSYSCONFDIR so we know where to find pg_service.conf
BEGIN { $ENV{PGSYSCONFDIR} = '/usr/local/meta/etc'; };

# Config {{{
my $db_service       = $config->{'DB_SERVICE'};
my $THUMB_LOCATION   = $config->{THUMB_LOCATION};
my $STORABLE         = $config->{'STORABLE'};
my $LOCKFILE         = $config->{'LOCK_FILE'};
my $DOWNLOAD_LOG     = $config->{'DOWNLOAD_LOG'};
my $WGET             = "/usr/bin/wget";
my $NAME             = $config->{'LOG_FILE'};
my @FILES;
# }}}

my $href;

# If we are already running, just exit quietly
open( LOCK, ">$LOCKFILE" ) || die log_out( LOG_WARN, "open($LOCKFILE): $!" );
if ( !flock( LOCK, LOCK_EX | LOCK_NB ) ) {
	exit( 0 );
}

# Be nice to the server
nice( 20 );

# Obviously $WGET has to exist for us to run ..
die log_out( LOG_WARN, "Can not run without: $WGET" ) if ( !-x $WGET );

log_out( LOG_INFO, "$NAME start" );

# Our thumbnail storable
my $thumbs = {};

# If the storable exists, load it
if ( -r $STORABLE ) {
	$thumbs = retrieve( $STORABLE ) || {};
}

# delete temp log file for wget (might be left over if crashed last time?)
unlink( $DOWNLOAD_LOG );

# How many files do we have to grab?
my $files  = 0;

my $dbi = DBI->connect( 'dbi:Pg:service='.$db_service, undef, undef, { 'RaiseError' => 0, PrintError => 0 } ) || die log_out( LOG_WARN, $DBI::errstr );

my @thumbs_aref;

# Main {{{
# - Grab the thumbnails we need from DB, exit if none.
# - download them to Meta server.
# - Then check to see if we downloaded them
# - update DB

if (defined $config->{'DL_GROUPING'} ) {
	# Code for download sites.
	get_dl_thumbs();
	download_thumbs();	
	check_dl_thumbs();	
}

# Lets store the results
store( $thumbs, $STORABLE );

# xfer wget log msgs to our log file
xfer_wget_log();

log_out( LOG_INFO, "$NAME end" );

# }}}


# subs {{{
# sub get_dl_thumbs
# Ok, lets run through the list and see which ones we need
sub get_dl_thumbs {

	my $SQL = $config->{'SQL'};
	my $grouping = $config->{'DL_GROUPING'}; # group the hash.
	$href = $dbi->selectall_hashref( $SQL, $grouping ) || die log_out( LOG_WARN, $DBI::errstr );
	log_out( LOG_INFO, "Running through file list .." );
	foreach my $movie_id ( keys %{ $href } ) {
		foreach my $thumbnail_type ( keys %{ $href->{$movie_id} } ) {
			foreach my $user_level ( keys %{ $href->{$movie_id}->{$thumbnail_type} } ) {
				foreach my $thumbnail_order ( keys %{ $href->{$movie_id}->{$thumbnail_type}->{$user_level} } ) {
					foreach my $thumbnail_size ( keys %{ $href->{$movie_id}->{$thumbnail_type}->{$user_level}->{$thumbnail_order} } ) {
						my $mh = $href->{$movie_id}->{$thumbnail_type}->{$user_level}->{$thumbnail_order}->{$thumbnail_size}; 
						my $th = $thumbs->{$movie_id}->{$thumbnail_type}->{$user_level}->{$thumbnail_order}->{$thumbnail_size}; 

						# Is the movie unchanged?
						if ( defined($th->{thumbnail_md5}) && $mh->{thumbnail_md5} eq $th->{thumbnail_md5} && $mh->{has_thumbnail} == 1 ) {
							# We don't need the movie, so drop it from our list
							delete $href->{$movie_id}->{$thumbnail_type}->{$user_level}->{$thumbnail_order}->{$thumbnail_size}; 
							next;
						}

						# Ok, it needs to be updated, add it to the list to grab
						my $fdir="$THUMB_LOCATION/${thumbnail_type}_${user_level}_${thumbnail_order}_${thumbnail_size}/";
						# make array of hashes
						if ( ${thumbnail_type} == 2 ) {
							push @thumbs_aref,{
							'url' =>"$mh->{thumbnail_url}",
							'file'=>"$fdir/${movie_id}.gif"
							};
						} else {
							push @thumbs_aref,{
							'url' => "$mh->{thumbnail_url}",
							'file'=> "$fdir/${movie_id}.jpg"
							};
						}
						++$files;
					}
				}
			}
		}
	}

	# If there are no files to get, just quietly exit
	if ( $files < 1 ) {
		log_out( LOG_INFO, "No files to download" );
		exit( 0 );
	}
}

# sub download_thumbs
# Download the files from the array with wget
sub download_thumbs {
	log_out( LOG_INFO, "Calling wget with $files file(s) .." );

	# Ok, lets ask wget to download the files for us ..
	my $tmp_file = $config->{'TMP_WGET_FILE'};
	foreach my $img ( @thumbs_aref  ) {
		# make sure it doesn't exist before downloading.
		unlink $tmp_file;
		# system call for wget
		system( $WGET,'-a',$DOWNLOAD_LOG,$img->{'url'},'-O',$tmp_file,"--user-agent='TAR Thumbnail Fetcher v0.2'");
		# Don't save empty files
		if (-r $tmp_file && -s $tmp_file > 0) {
			my $now = time; # time could change for each round.
			utime $now, $now, $tmp_file;
			if (! rename ($tmp_file,$img->{'file'}) ) {
				log_out( LOG_WARN,"Cannot rename $tmp_file to ". $img->{'file'} );
			}
		}
	}
	# clean up after we're done.
	unlink $tmp_file;
}

# sub check_dl_thumbs
# check to see that the dl files were downloaded and update the DB.
sub check_dl_thumbs {
	my $movie_update_sql = $config->{'MOVIE_UPDATE'};
	my $ppv_update_sql   = $config->{'PPV_UPDATE'};
	# Update flag
	my $movie_sth = $dbi->prepare( $movie_update_sql ) || die log_out( LOG_WARN, $DBI::errstr );
	my $ppv_sth = $dbi->prepare( $ppv_update_sql ) || die log_out( LOG_WARN, $DBI::errstr );

	# Now lets run through the list an ensure the files are all there
	foreach my $movie_id ( keys %{ $href } ) {
		foreach my $thumbnail_type ( keys %{ $href->{$movie_id} } ) {
			foreach my $user_level ( keys %{ $href->{$movie_id}->{$thumbnail_type} } ) {
				foreach my $thumbnail_order ( keys %{ $href->{$movie_id}->{$thumbnail_type}->{$user_level} } ) {
					foreach my $thumbnail_size ( keys %{ $href->{$movie_id}->{$thumbnail_type}->{$user_level}->{$thumbnail_order} } ) {
						my $mh = $href->{$movie_id}->{$thumbnail_type}->{$user_level}->{$thumbnail_order}->{$thumbnail_size}; 

						my $fname;
						if ( ${thumbnail_type} == 2 ) {
							$fname="$THUMB_LOCATION/${thumbnail_type}_${user_level}_${thumbnail_order}_${thumbnail_size}/${movie_id}.gif";
						} else {
							$fname="$THUMB_LOCATION/${thumbnail_type}_${user_level}_${thumbnail_order}_${thumbnail_size}/${movie_id}.jpg";
						}

						# Does the file exist?
						if ( !-r $fname ) {
							log_out( LOG_WARN, "Unable to download thumbnail for: $movie_id, URL failed to get $mh->{thumbnail_url}" );
							# It doesn't exist, so we skip adding it to the storable.
							# Hopefully the next run it'll exist and be fixed
							next;
						}

						# Does the MD5 match what they actually claim it to be?
						my $digest = file_md5_hex( $fname );

						# Did we get a matching file
						if ( ! defined $mh->{thumbnail_md5} || ( $digest ne $mh->{thumbnail_md5} ) ) {
							if ( ! defined $mh->{thumbnail_md5} ) {
								log_out( LOG_WARN, "Movie MD5 (movie_id $movie_id) check failed, database is NULL, actual file is $digest" );
							}
							else {
								log_out( LOG_WARN, "Movie MD5 (movie_id $movie_id) check failed, database claims $mh->{thumbnail_md5}, actual file is $digest" );
							}
							# We should eventually add a "next" here, but for now because we know the database is so screwed up, we'll allow it
							# through until they can fix the system properly.
						}

						# Set the details (md5 of the actual file) in the storable
						$thumbs->{$movie_id}->{$thumbnail_type}->{$user_level}->{$thumbnail_order}->{$thumbnail_size}->{thumbnail_md5} = $digest; 

						# Update database that we have the file (if needed)
						if ( $mh->{has_thumbnail} == 0 ) {
							if ( $mh->{prod_type} == 3 ) {
								if ( !$movie_sth->execute( $movie_id, $thumbnail_type, $user_level, $thumbnail_order, $thumbnail_size ) ) {
									log_out( LOG_WARN, "Cannot update thumbnail flag for movie $movie_id: $DBI::errstr" );
								}
							}
							elsif ( $mh->{prod_type} == 2 ) {
								if ( !$ppv_sth->execute( $movie_id, $thumbnail_type, $user_level, $thumbnail_order, $thumbnail_size ) ) {
									log_out( LOG_WARN, "Cannot update thumbnail flag for ppv $movie_id: $DBI::errstr" );
								}
							}
						}

						log_out( LOG_INFO, "Downloaded thumbnail for meta_id: $movie_id from URL: $mh->{thumbnail_url}" );
					}
				}
			}
		}
	}
}

# sub xfer_wget_log {{{
# put the wget log file into the meta log file.
sub xfer_wget_log {
	# check if there is something in the wget log file
	if ( ! ( -r $DOWNLOAD_LOG and -s $DOWNLOAD_LOG ) ) {
		log_out( LOG_INFO, "Nothing in wget log file" );
	}
	else {
		log_out( LOG_INFO, "Start msgs from wget log");
		open( IN, "<$DOWNLOAD_LOG" ) || die log_out( LOG_WARN, "open ($DOWNLOAD_LOG): $!\n" );
		while( my $line = <IN> ) {
			chomp( $line );
			log_out( LOG_INFO, $line );
		}
		close( IN );
		log_out( LOG_INFO, "End msgs from wget log");
	}
	unlink( $DOWNLOAD_LOG );
	return;
}
# }}}
