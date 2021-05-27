#!/usr/bin/perl -w
BEGIN { $ENV{PGSYSCONFDIR} = '/usr/local/meta/etc'; }

use strict;
use DBI;
use Fcntl qw( :flock );
use POSIX qw( nice mktime );
use Encode qw/from_to encode decode/;
use Time::Local;
use Config::Auto;
use EP;
use Data::Dumper;

# preparation {{{
# from config {{{
my $config    = Config::Auto::parse('/usr/local/meta/etc/ecp.cfg', format => "perl");
my $origin    = 'ecp';
my $origin_id = $config->{id}->{origin};
my $site_ids  = $config->{id}->{site};
my $urls      = $config->{url};
my $binary    = $config->{binary}; 
my $thumb_types = $config->{thumb_types}; 
my $file      = $config->{file_dir}.$origin.'.full.';
my $jst       = time() + (60 * 60 * 9);
# }}}
# Set log
log_set_defaults();
log_tie_stderr();

# database related {{{

###
### DBI connect parameters
###   connect(DSN, user, pass, options);
###   In our case here, we define db connection specifics in my.cnf entry below.  
###     That's why user/pass are undef.  undef is used as a placeholder so that we can 
###     provide the 4th argument(options).  In the options hash, we are using HandleError
###     to provide an anonymous subroutince to handle all failures.  Using this approach,
###     essentially, we setup a try/catch around every DBI statement.  It's more 
###     convenient than using eval oround each statement or die on each statement.
###

my $dbi = DBI->connect( "dbi:mysql:mysql_read_default_file=$ENV{PGSYSCONFDIR}/my.cnf;mysql_read_default_group=$origin",
	undef,
	undef,
	{
		RaiseError  => 0,
		PrintError  => 0,
		HandleError =>
		sub {
			my ( $error, $dbh) = @_;
			if ( $_[0] ) {
				log_out( LOG_ERR, "$_[0]" );
			} else {
				log_out( LOG_ERR, "Unable to connect to db" );
			}
			exit 255;
		}
	}
) ;

my $SQL = "SELECT * FROM ";
# }}}
my $LOCKFILE  = '/usr/local/meta/shared/meta_'.$origin.'_full.lock';
my ( $str, $movies, $files, $pages, $mref, $BYTES, $buffer, $ppvs, $ppv_files, $ppv_pages );
my ( $m_thumbs, $p_thumbs ) = ( {}, {} );
# }}}

open( LOCK, ">$LOCKFILE" ) || die log_out( LOG_WARN, "open($LOCKFILE): $!" );
exit( 0 ) if ( !flock( LOCK, LOCK_EX | LOCK_NB ) );
nice( 20 );

log_out( LOG_INFO, $origin."_full start" );

# create movie detail hashes {{{
my $thumbnails = get_details( 'tar_thumbnails' );
my $details    = get_details( 'movie_detail' );
my $attributes = get_details( 'movie_attributes' );
my $tar_flag   = get_details( 'tar_flag' );
my $ecp_tag    = get_details( 'movie_category' );
my $series_h   = get_details( 'series_movie' );
# }}}

# main {{{
# Get data from origin
get_movies( 'movie' );
get_files( 'tar_movies' );
bytes( $movies, 'movies' ) if ( defined $movies );
bytes( $pages, 'movie_pages' ) if ( defined $pages );
bytes( $files, 'movie_files' ) if ( defined $files );
bytes( $ppvs, 'ppvs' ) if ( defined $ppvs );
bytes( $ppv_pages, 'ppv_pages' ) if ( defined $ppv_pages );
bytes( $ppv_files, 'ppv_files' ) if ( defined $ppv_files );
bytes( $m_thumbs, 'movie_thumbs' ) if ( defined $m_thumbs );
bytes( $p_thumbs, 'ppv_thumbs' ) if ( defined $p_thumbs );
create_file() if ( defined $BYTES );
# }}}

log_out( LOG_INFO, $origin."_full end" );

# sub get_details {{{
sub get_details {
	my ( $word, $sql, $sth, $href ) = shift @_;
	log_out( LOG_INFO, "Getting details from $word" );

	if ( defined $word ) {
		# tar_thumbnails {{{
		if ( $word eq 'tar_thumbnails' ) {
			#remove product_type when adding hey_douga
			$sql = $SQL.$word." WHERE flashimage_flag = 1 or imagerotation_flag or primary_flag > 0 and product_type IN (1,2,3); ";
			$sth = $dbi->prepare( $sql );
			$sth->execute();
			my ( $primary, $temp );
			my $thumb = $sth->fetchall_hashref( [ qw( movie_seq file_number) ] );
			if ( keys( %{ $thumb } ) < 1 ) {
				die log_out( LOG_WARN, "No data returned from $word" );
			}
			for my $seq ( keys %{ $thumb } ) {
				for my $fid ( keys %{ $thumb->{ $seq } } ) {
					my $data = $thumb->{ $seq }->{ $fid };
					if ( !defined $data->{status} ) {
						log_out( LOG_WARN, $word.".status undefined (file_number $fid)" );
						delete( $thumb->{ $seq }->{ $fid } );
					} elsif ( !defined $data->{primary_flag} ) {
						log_out( LOG_WARN, $word."primary_flag undefined (file_number $fid)" );
						delete( $thumb->{ $seq }->{ $fid } );
					} elsif ( $data->{status} > 2 ) {
						log_out( LOG_WARN, "Unknown thumbnail status found - $data->{status} (file_number $fid)" );
						delete( $thumb->{ $seq }->{ $fid } );
					} elsif ( $data->{primary_flag} > 2 ) {
						log_out( LOG_WARN, "Unknown thumbnail primary_flag found - $data->{primary_flag} (file_number $fid)" );
						delete( $thumb->{ $seq }->{ $fid } );
					} else {
						if ( defined $thumb->{ $seq }->{ $fid } ) {
							# Remove any invalid data
							while ( my( $key, $val ) = each( %{ $data } ) ) {
								if ( $key eq 'movie_seq' || $key eq'server_name' || $key eq 'path' || $key eq 'md5' ) {
									if ( !defined $val ) {
										log_out( LOG_WARN, "$key undefined ($word file_number $fid)" );
										delete( $thumb->{ $seq }->{ $fid } );
									}
								}
							}
							$temp->{ $seq }->{ $fid }->{p_type} = $data->{product_type};

							#flashimage_flag, url, md5 added Dec 6, 2011	
							if ( $data->{flashimage_flag} == 1) {
								$temp->{ $seq }->{ $fid }->{fl_f} = $data->{flashimage_flag};
								$temp->{ $seq }->{ $fid }->{fl_url} = 'http://'.$data->{server_name}.$data->{path};
								$temp->{ $seq }->{ $fid }->{fl_md5} = $data->{md5};
								$temp->{ $seq }->{ $fid }->{fl_status} = $data->{status};
								$temp->{ $seq }->{ $fid }->{fl_ptype} = $data->{product_type};
							}
							if ( $data->{imagerotation_flag} == 1) {
								$temp->{ $seq }->{ $fid }->{ir_f} = $data->{imagerotation_flag};
								$temp->{ $seq }->{ $fid }->{ir_url} = 'http://'.$data->{server_name}.$data->{path};
								$temp->{ $seq }->{ $fid }->{ir_md5} = $data->{md5};
								$temp->{ $seq }->{ $fid }->{ir_status} = $data->{status};
								$temp->{ $seq }->{ $fid }->{ir_ptype} = $data->{product_type};
							}
							if ( $data->{primary_flag} == 2) {
								$temp->{ $seq }->{ $fid }->{cen_f} = 1;
								$temp->{ $seq }->{ $fid }->{cen_url} = 'http://'.$data->{server_name}.$data->{path};
								$temp->{ $seq }->{ $fid }->{cen_md5} = $data->{md5};
								$temp->{ $seq }->{ $fid }->{cen_status} = $data->{status};
								$temp->{ $seq }->{ $fid }->{cen_ptype} = $data->{product_type};
							}
							if ( $data->{primary_flag} == 1) {
								$temp->{ $seq }->{ $fid }->{uncen_f} = 1;
								$temp->{ $seq }->{ $fid }->{uncen_url} = 'http://'.$data->{server_name}.$data->{path};
								$temp->{ $seq }->{ $fid }->{uncen_md5} = $data->{md5};
								$temp->{ $seq }->{ $fid }->{uncen_status} = $data->{status};
								$temp->{ $seq }->{ $fid }->{uncen_ptype} = $data->{product_type};
							}
						}
					}
				}
			}
			for my $seq ( keys %{ $temp } ) {
				for my $fid ( keys %{ $temp->{ $seq } } ) {
					my $data = $temp->{ $seq }->{ $fid };

					#flashimage_flag, url, md5 added Dec 6, 2011
					if ( defined $data->{fl_f} && $data->{fl_f} == 1 && not exists $href->{ $seq }->{fl_f} ) {
						$href->{ $seq }->{ $data->{p_type} }->{fl_f} = $data->{fl_f};	
						$href->{ $seq }->{ $data->{p_type} }->{fl_url} = $data->{fl_url};	
						$href->{ $seq }->{ $data->{p_type} }->{fl_md5} = $data->{fl_md5};
						$href->{ $seq }->{ $data->{p_type} }->{fl_status} = $data->{fl_status};
					}
					if ( defined $data->{ir_f} && $data->{ir_f} == 1 && not exists $href->{ $seq }->{ir_f} ) {
						$href->{ $seq }->{ $data->{p_type} }->{ir_f} = $data->{ir_f};	
						$href->{ $seq }->{ $data->{p_type} }->{ir_url} = $data->{ir_url};	
						$href->{ $seq }->{ $data->{p_type} }->{ir_md5} = $data->{ir_md5};
						$href->{ $seq }->{ $data->{p_type} }->{ir_status} = $data->{ir_status};
					}
					if ( defined $data->{cen_f} && $data->{cen_f} == 1 && not exists $href->{ $seq }->{cen_f} ) {
						$href->{ $seq }->{ $data->{p_type} }->{cen_f} = $data->{cen_f};	
						$href->{ $seq }->{ $data->{p_type} }->{cen_url} = $data->{cen_url};	
						$href->{ $seq }->{ $data->{p_type} }->{cen_md5} = $data->{cen_md5};
						$href->{ $seq }->{ $data->{p_type} }->{cen_status} = $data->{cen_status};
					}
					if ( defined $data->{uncen_f} && $data->{uncen_f} == 1 && not exists $href->{ $seq }->{uncen_f} ) {
						$href->{ $seq }->{ $data->{p_type} }->{uncen_f} = $data->{uncen_f};	
						$href->{ $seq }->{ $data->{p_type} }->{uncen_url} = $data->{uncen_url};	
						$href->{ $seq }->{ $data->{p_type} }->{uncen_md5} = $data->{uncen_md5};
						$href->{ $seq }->{ $data->{p_type} }->{uncen_status} = $data->{uncen_status};
					}
				}
			}
			undef( $temp );
		}
		# }}}
		# movie_detail {{{
		if ( $word eq 'movie_detail' ) {
			$sql = $SQL.$word." WHERE group_id IN (1,2) ORDER BY group_id DESC;";
			$sth = $dbi->prepare( $sql );
			$sth->execute();
			$href   = $dbi->selectall_hashref( $sth, 'movie_seq' );
		}
		# }}}
		# movie_attributes {{{
		if ( $word eq 'movie_attributes' ) {
			$sql = $SQL.$word." WHERE attribute_id = 4;";
			$sth = $dbi->prepare( $sql );
			$sth->execute();
			$href   = $dbi->selectall_hashref( $sth, 'movie_seq' );
		}
		# }}}
		# tar_flag {{{
		if ( $word eq 'tar_flag' ) {
			$sql = $SQL.$word.";";
			$sth = $dbi->prepare( $sql );
			$sth->execute();
			my $ref = $sth->fetchall_hashref( [ qw( product_type movie_seq ) ] );
			# We don't import PPV or Monthly yet, so we care only product type 1
			# 1 - ECP, 2 - PPV, 3 - Monthly
			for my $ptype ( keys %{ $ref } ) {
				for my $seq ( keys %{ $ref->{ $ptype } } ) {
					my $data = $ref->{ $ptype }->{ $seq };
					if ( $ptype == 1 || $ptype == 3 || $ptype == 2) {
						$href->{ $seq }->{ $data->{product_type} }->{v}  = $data->{vip_flag};
						$href->{ $seq }->{ $data->{product_type} }->{sv} = $data->{supervip_flag};
					}
				}
			}
		}
		# }}}
		# category {{{
		if ( $word eq 'movie_category' ) {
			$sql = $SQL.$word.";";
			$sth = $dbi->prepare( $sql );
			$sth->execute();
			$href  = $sth->fetchall_arrayref({});
			my $tag;
			for my $key ( @{ $href } ) {
				push @{ $tag->{ $key->{movie_seq} } }, int( $key->{category_id} );
			}
			return $tag;
		}	
		# }}}

		# series_movie {{{
		if ( $word eq 'series_movie' ) {
			$sql = $SQL.$word." ORDER by movie_seq, series_id;";
			$sth = $dbi->prepare( $sql ) || die log_out( LOG_ERR, "Error preparing $sql: $!" );
			$sth->execute() || die log_out( LOG_ERR, "Error executing $sql: $!" );;
			my $series;
			while( my @rows = $sth->fetchrow_array ) {
				if ( defined $rows[0] && defined $rows[1] ) {
					push @{ $series->{ $rows[1] } } , $rows[0];
				}
			}
			return $series;
		}
		# }}}
	}
	return $href;
}
# }}}

# sub get_movies {{{
sub get_movies {
	my ( $table, $sql, $href, $href_mf, $flag_h ) = shift @_;
	log_out( LOG_INFO, "Getting movie data.." );

	$sql = $SQL.$table.";";
	my $sth = $dbi->prepare( $sql );
	$sth->execute();
	$href  = $dbi->selectall_hashref( $sth, 'movie_seq' );

	# Get movie data {{{	
	for my $id ( keys %{ $href } ) {
		my $data   = $href->{ $id };
		if ( !defined $site_ids->{ $data->{site_id} } ) {
			# This is not mapped in the config
			log_out( LOG_WARN, "This referrer id ( $data->{site_id} ) is not mapped in the config file for movie_seq ( $id )" );
			delete( $href->{ $id } );
			next;
		}
		# Check configured sites only
		if ( defined $urls->{domain}->{ $site_ids->{ $data->{site_id} } } || ( $data->{ppv_status} && $data->{approve_status} ) ) {

			if ( !defined $tar_flag->{ $id } ) {
				# This movie needs tar_flag to process, which is on a timer
				log_out( LOG_INFO, "No record in tar_flag table: movie_seq $id" );
				delete( $href->{ $id } );
			# Monthlys and ppvs must be approved.
			} elsif ( ( $data->{ppv_status} == 1 || $data->{monthly_status} == 1 ) && $data->{approve_status} != 1 ) {
				delete( $href->{ $id } );
			} elsif ( !defined $data->{name} || $data->{name} eq '' ) {
				log_out( LOG_WARN, "movie title undefined ($table): movie_seq $id" );
				delete( $href->{ $id } );
			} else {
				# Check invalid data {{{
				if ( defined $data->{ecp_status} && $data->{ecp_status} != 1 && $data->{ecp_status} != 0 ) {
					log_out( LOG_WARN, "Unknown ecp_status found ($table): " . $data->{ecp_status} . " (movie_seq $id)" );
					delete( $href->{ $id } );
					next;
				# Jira ISD-117 if both ecp and ppv then warn
				} elsif ( $data->{monthly_status} == 1 && $data->{ecp_status} == 1) {
					log_out( LOG_WARN, "Movie is both monthly and ECP ($table): movie_seq $id");
					delete( $href->{ $id } );
					next;
				} 
				# jira ISD-117 if it has no flags then delete.
				if (( $data->{ecp_status} != 1 ) && ( $data->{monthly_status} != 1 ) && ( $data->{ppv_status} != 1 )) {
					delete( $href->{ $id } );
					next;
				}
				# If it's an ECP movie
				if ( $data->{ecp_status} == 1 ) {
					# We do not import future movies
					if ( defined $data->{ecp_start_date} && $data->{ecp_start_date} ne '0000-00-00' && $data->{ecp_start_date} lt '2038-01-01' ) {
						valid_date( $href, $id, $data->{ecp_start_date} );
						my $epoch = convert( $data->{ecp_start_date} ) if ( defined $href->{ $id } );
						if ( !defined $epoch || $epoch == 0 ) {
							log_out( LOG_WARN, "Bad ecp_start_date " . $data->{ecp_start_date} . " for movie $id" );
							delete( $href->{ $id } );
						} else {
							# jira ISD-117, msd is for the movies' hash.
							$href->{ $id }->{msd} = $epoch;
						}
					} else {
						delete( $href->{ $id } );
					}
					# We need to set expired condition for these movies.
					# (ECP ecp_end_date < now())
					if ( defined $data->{ecp_end_date} && $data->{ecp_end_date} ne '0000-00-00' && $data->{ecp_end_date} lt '2038-01-01' ) {
						valid_date( $href, $id, $data->{ecp_end_date} );
						my $epoch = convert( $data->{ecp_end_date} ) if ( defined $href->{ $id } );
						if ( defined $epoch && $epoch != 0 ) {
							$href->{ $id }->{ed} = $epoch;
						}
					}
				# If it's a non-ECP movie (monthly and/or ppv)
				} else {
					# if it's a ppv movie
					if ( $data->{ppv_status} == 1 ) {
						if ( defined $data->{start_date}  && $data->{start_date} ne '0000-00-00' && $data->{start_date} lt '2038-01-01' ) {
							valid_date( $href, $id, $data->{start_date} );
							my $epoch = convert( $data->{start_date} ) if ( defined $href->{ $id } );
							if ( !defined $epoch || $epoch == 0 ) {
								delete( $href->{ $id } );
							} else {
								# jira ISD-117 psd is the start_date for the ppvs' hash.
								$href->{ $id }->{psd} = $epoch;
							}
						} else {
							delete( $href->{ $id } );
						}
					}
					# If it's a non-ECP monthly movie
					if ( $data->{monthly_status} == 1 ) {
						if ( defined $data->{monthly_start_date} && $data->{monthly_start_date} ne '0000-00-00' && $data->{monthly_start_date} lt '2038-01-01' ) {
							valid_date( $href, $id, $data->{monthly_start_date} );
							my $epoch = convert( $data->{monthly_start_date} ) if ( defined $href->{ $id } );
							if ( !defined $epoch || $epoch == 0 ) {
								delete( $href->{ $id } );
							} else {
								$href->{ $id }->{msd} = $epoch;
							}
						} else {
							delete( $href->{ $id } );
						}
					}
					# jira ISD-117 both ppv and monthly
					# have the same end_date
					if ( defined $data->{end_date} && $data->{end_date} ne '0000-00-00' && $data->{end_date} lt '2038-01-01' ) {
						valid_date( $href, $id, $data->{end_date} );
						my $epoch = convert( $data->{end_date} ) if ( defined $href->{ $id } );
						if ( defined $epoch && $epoch != 0 ) {
							$href->{ $id }->{ed} = $epoch;
						}
					}
				} # }}}
			}
		}
	}
	# }}}
	# Create movies hash {{{	
	for my $id ( keys %{ $href } ) {
		my $thumb;
		my $data   = $href->{ $id };
		my $data_mf = $href_mf->{ $id };
		if ( defined $urls->{domain}->{ $site_ids->{ $data->{site_id} } } || ( $data->{ppv_status} == 1 && $data->{approve_status} == 1 ) ) {
			
			$movies->{ $id }->{is_ppv} = $data->{ppv_status};
			$movies->{ $id }->{is_monthly} = ( $urls->{domain}->{ $site_ids->{ $data->{site_id} } } ) ? $data->{monthly_status} : 0;
			if ( $data->{ppv_status} == 1 ) {
				$movies->{ $id }->{meta_org} = ( defined $site_ids->{ $data->{site_id} } ) ? $site_ids->{ $data->{site_id} } : '';
				$movies->{ $id }->{org} = $data->{site_id};
				$movies->{ $id }->{lorg} = length( $movies->{ $id }->{meta_org} );
				if ( $attributes->{ $id } && $attributes->{ $id }->{attribute_value} ) {
					$movies->{ $id }->{pr} = sprintf( "%.2f", $attributes->{ $id }->{attribute_value});
				} else {
					$movies->{ $id }->{pr} = sprintf( "%.2f", 0 );
				}
				$movies->{ $id }->{lpr} = length( $movies->{ $id }->{pr} );
			}
			if ( $data->{ppv_status} && $data->{approve_status} && ( $data->{monthly_status} || $data->{ecp_status} ) ) {
				$movies->{ $id }->{lorigin} =  $origin_id;
				$movies->{ $id }->{loriginal} = $id;
			} else {
				$movies->{ $id }->{lorigin} =  '';
				$movies->{ $id }->{loriginal} = '';
			}

			$movies->{ $id }->{llorigin} =  length( $movies->{ $id }->{lorigin} );
			$movies->{ $id }->{lloriginal} = length( $movies->{ $id }->{loriginal} );
			$movies->{ $id }->{s}  = $site_ids->{ $data->{site_id} };
			$movies->{ $id }->{i}  = encode( 'utf8', decode( 'euc-jp', $data->{name} ) );
			$movies->{ $id }->{an} = ( defined $data->{act} ) ? encode( 'utf8', decode( 'euc-jp', $data->{act} ) ) : '';
			$movies->{ $id }->{m}  = $data->{movie_id};
			$movies->{ $id }->{lmvid} = length( $movies->{ $id }->{m} );
			# jira ISD-117 assign the correct start date for movies
			$movies->{ $id }->{sd} = $data->{msd};
			$movies->{ $id }->{li} = length( $movies->{ $id }->{i} );
			$movies->{ $id }->{la} = ( defined $movies->{ $id }->{an} ) ? length( $movies->{ $id }->{an} ) : 0;
			$movies->{ $id }->{dr} = encode( 'utf8', decode('euc-jp', $details->{$id}->{duration}) );
			if ( defined $movies->{ $id }->{dr} ) {
				if ( not $movies->{ $id }->{dr} =~ /^[+-]?\d+$/ ) {
					$movies->{ $id }->{dr} = 0;
				}
			} else {
				$movies->{ $id }->{dr} = 0;
			}
			$movies->{ $id }->{ldr} = length( $movies->{ $id }->{dr} );

			# Thumbnail
			my $tdata;
			if ( $data->{ecp_status} == 1 ) {
				if ( defined $thumbnails->{ $id }->{1} ) {
					$tdata = $thumbnails->{ $id }->{1};
				}
			} 
			elsif ( $data->{approve_status} == 1 && $movies->{ $id }->{is_ppv} == 1 ) {
				if ( $movies->{ $id }->{is_monthly} == 0 && defined $thumbnails->{ $id }->{2} ) {
					$tdata = $thumbnails->{ $id }->{2};
				}
				elsif ( defined $thumbnails->{ $id }->{3} ) {
					$tdata = $thumbnails->{ $id }->{3};
				}
			}
			elsif ( $data->{approve_status} == 1 && $movies->{ $id }->{is_ppv} == 0 ) {
				if ( $movies->{ $id }->{is_monthly} == 1 && defined $thumbnails->{ $id }->{3} ) {
					$tdata = $thumbnails->{ $id }->{3};
				}
			}

			if ( $tdata ) {
				# Find thumb to use for legacy
				if ( $tdata->{cen_f} && $tdata->{cen_status} == 1) {
					undef $thumb;
					$thumb->{url} = $tdata->{cen_url};			
					$thumb->{md5} = $tdata->{cen_md5};
					$thumb->{level} = 0;
					$thumb->{order} = 0;
					$thumb->{size} = 0;
					$thumb->{flags} = ( $tdata->{cen_status} == 1 ) ? 1 : 0;
					$thumb->{l_url} = length( $thumb->{url} );
					$thumb->{l_md5} = length( $thumb->{md5} );
					$thumb->{tl} = $thumb->{l_url} + $thumb->{l_md5} + $binary->{ 'movie_thumbs' }->{bytes};
					$m_thumbs->{ $id }->{ $thumb_types->{'legacy'} } = $thumb;
				}
				elsif ( $tdata->{uncen_f} && $tdata->{uncen_status} == 1) {
					undef $thumb;
					$thumb->{url} = $tdata->{uncen_url};			
					$thumb->{md5} = $tdata->{uncen_md5};
					$thumb->{level} = 0;
					$thumb->{order} = 0;
					$thumb->{size} = 0;
					$thumb->{flags} = ( $tdata->{uncen_status} == 1 ) ? 1 : 0;
					$thumb->{l_url} = length( $thumb->{url} );
					$thumb->{l_md5} = length( $thumb->{md5} );
					$thumb->{tl} = $thumb->{l_url} + $thumb->{l_md5} + $binary->{ 'movie_thumbs' }->{bytes};
					$m_thumbs->{ $id }->{ $thumb_types->{'legacy'} } = $thumb;
				}
	
				elsif ( $tdata->{cen_f} && $tdata->{cen_status} == 2) {
					undef $thumb;
					$thumb->{url} = $tdata->{cen_url};			
					$thumb->{md5} = $tdata->{cen_md5};
					$thumb->{level} = 0;
					$thumb->{order} = 0;
					$thumb->{size} = 0;
					$thumb->{flags} = ( $tdata->{cen_status} == 1 ) ? 1 : 0;
					$thumb->{l_url} = length( $thumb->{url} );
					$thumb->{l_md5} = length( $thumb->{md5} );
					$thumb->{tl} = $thumb->{l_url} + $thumb->{l_md5} + $binary->{ 'movie_thumbs' }->{bytes};
					$m_thumbs->{ $id }->{ $thumb_types->{'legacy'} } = $thumb;
				}
				elsif ( $tdata->{uncen_f} && $tdata->{uncen_status} == 2) {
					undef $thumb;
					$thumb->{url} = $tdata->{uncen_url};			
					$thumb->{md5} = $tdata->{uncen_md5};
					$thumb->{level} = 0;
					$thumb->{order} = 0;
					$thumb->{size} = 0;
					$thumb->{flags} = ( $tdata->{uncen_status} == 1 ) ? 1 : 0;
					$thumb->{l_url} = length( $thumb->{url} );
					$thumb->{l_md5} = length( $thumb->{md5} );
					$thumb->{tl} = $thumb->{l_url} + $thumb->{l_md5} + $binary->{ 'movie_thumbs' }->{bytes};
					$m_thumbs->{ $id }->{ $thumb_types->{'legacy'} } = $thumb;
				}

				elsif ( $tdata->{cen_f} && $tdata->{cen_status} == 0) {
					undef $thumb;
					$thumb->{url} = $tdata->{cen_url};			
					$thumb->{md5} = $tdata->{cen_md5};
					$thumb->{level} = 0;
					$thumb->{order} = 0;
					$thumb->{size} = 0;
					$thumb->{flags} = ( $tdata->{cen_status} == 1 ) ? 1 : 0;
					$thumb->{l_url} = length( $thumb->{url} );
					$thumb->{l_md5} = length( $thumb->{md5} );
					$thumb->{tl} = $thumb->{l_url} + $thumb->{l_md5} + $binary->{ 'movie_thumbs' }->{bytes};
					$m_thumbs->{ $id }->{ $thumb_types->{'legacy'} } = $thumb;
				}
				elsif ( $tdata->{uncen_f} && $tdata->{uncen_status} == 0) {
					undef $thumb;
					$thumb->{url} = $tdata->{uncen_url};			
					$thumb->{md5} = $tdata->{uncen_md5};
					$thumb->{level} = 0;
					$thumb->{order} = 0;
					$thumb->{size} = 0;
					$thumb->{flags} = ( $tdata->{uncen_status} == 1 ) ? 1 : 0;
					$thumb->{l_url} = length( $thumb->{url} );
					$thumb->{l_md5} = length( $thumb->{md5} );
					$thumb->{tl} = $thumb->{l_url} + $thumb->{l_md5} + $binary->{ 'movie_thumbs' }->{bytes};
					$m_thumbs->{ $id }->{ $thumb_types->{'legacy'} } = $thumb;
				}


				#flashimage_flag, url, md5 added Dec 6, 2011
				if ( $tdata->{fl_f} && $tdata->{fl_f} == 1 ) {
					undef $thumb;
					$thumb->{url} = $tdata->{fl_url};
					$thumb->{md5} = $tdata->{fl_md5};
					$thumb->{level} = 0;
					$thumb->{order} = 0;
					$thumb->{size} = 0;
					$thumb->{flags} = ( $tdata->{fl_status} == 1 ) ? 1 : 0;
					$thumb->{l_url} = length( $thumb->{url} );
					$thumb->{l_md5} = length( $thumb->{md5} );
					$thumb->{tl} = $thumb->{l_url} + $thumb->{l_md5} + $binary->{ 'movie_thumbs' }->{bytes};
					$m_thumbs->{ $id }->{ $thumb_types->{'flash'} } = $thumb;
				}

				if ( $tdata->{ir_f} && $tdata->{ir_f} == 1 ) {
					undef $thumb;
					$thumb->{url} = $tdata->{ir_url};
					$thumb->{md5} = $tdata->{ir_md5};
					$thumb->{level} = 0;
					$thumb->{order} = 0;
					$thumb->{size} = 0;
					$thumb->{flags} = ( $tdata->{ir_status} == 1 ) ? 1 : 0;
					$thumb->{l_url} = length( $thumb->{url} );
					$thumb->{l_md5} = length( $thumb->{md5} );
					$thumb->{tl} = $thumb->{l_url} + $thumb->{l_md5} + $binary->{ 'movie_thumbs' }->{bytes};
					$m_thumbs->{ $id }->{ $thumb_types->{'ir'} } = $thumb;
					$m_thumbs->{ $id }->{2} = $thumb;
				}

				if ( $tdata->{cen_f} ) {
					undef $thumb;
					$thumb->{url} = $tdata->{cen_url};			
					$thumb->{md5} = $tdata->{cen_md5};			
					$thumb->{level} = 0;
					$thumb->{order} = 0;
					$thumb->{size} = 0;
					$thumb->{flags} = ( $tdata->{cen_status} == 1 ) ? 1 : 0;
					$thumb->{l_url} = length( $thumb->{url} );
					$thumb->{l_md5} = length( $thumb->{md5} );
					$thumb->{tl} = $thumb->{l_url} + $thumb->{l_md5} + $binary->{ 'movie_thumbs' }->{bytes};
					$m_thumbs->{ $id }->{ $thumb_types->{'censored'} } = $thumb;
				}

				if ( $tdata->{uncen_f} ) {
					undef $thumb;
					$thumb->{url} = $tdata->{uncen_url};			
					$thumb->{md5} = $tdata->{uncen_md5};			
					$thumb->{level} = 0;
					$thumb->{order} = 0;
					$thumb->{size} = 0;
					$thumb->{flags} = ( $tdata->{uncen_status} == 1 ) ? 1 : 0;
					$thumb->{l_url} = length( $thumb->{url} );
					$thumb->{l_md5} = length( $thumb->{md5} );
					$thumb->{tl} = $thumb->{l_url} + $thumb->{l_md5} + $binary->{ 'movie_thumbs' }->{bytes};
					$m_thumbs->{ $id }->{ $thumb_types->{'uncensored'} } = $thumb;
				}
			}
			# Description
			if ( defined $details->{ $id } ) {
				$movies->{ $id }->{d}  = encode( 'utf8', decode( 'euc-jp', $details->{ $id }->{memo} ) );
				if ( !defined $movies->{ $id }->{d} ) {
					$movies->{ $id }->{d}  = '';
				}
				$movies->{ $id }->{ld} = ( defined $movies->{ $id }->{d} ) ? length( $movies->{ $id }->{d} ) : 0;
			} else {
				$movies->{ $id }->{d}  = '';
				$movies->{ $id }->{ld} = 0;
			}
			# Flag - is_enabled, has_sample, needs_key, is_expired
			# For now, we don't have no_sample/needs_key movies yet
			if ( defined $data->{ed} && $data->{ed} < $jst ) {
				# The movie is expired, set expired condition
				log_out( LOG_NOTICE, "movie_seq $id is expired, setting expired condition" );
				$flag_h->{ $id }->{ie} = 1;
			} else {
				$flag_h->{ $id }->{ie} = 0;
			}
			$flag_h->{ $id }->{en} = $data->{ecp_status} || $data->{monthly_status} || $data->{ppv_status};
			$movies->{ $id }->{en} = $data->{ecp_status} || $data->{monthly_status} || $data->{ppv_status};
			# We import future movies as disabled movies.
			# jira ISD-117 use monthly start date
			if ( defined $data->{msd} && $data->{msd} > $jst ) {
				$flag_h->{ $id }->{en} = 0;
				$movies->{ $id }->{en} = 0;
			}
			$flag_h->{ $id }->{hs} = 1;
			$flag_h->{ $id }->{nk} = 0;
		
			if ( defined $data->{recurring_flag} && $data->{recurring_flag} == 1 ) {
				$flag_h->{ $id }->{ir} = 1;
			} else {
				$flag_h->{ $id }->{ir} = 0;
			}
			# no_advertisement flag
			# true if movie.meta_cond_disable = 1.
			if ( defined $data->{meta_cond_disable} && $data->{meta_cond_disable} == 1 ) {
				$flag_h->{ $id }->{na} = 1;
			} else {
				$flag_h->{ $id }->{na} = 0;
			}
		}
	}
	# }}}
	my $ppvs_f;
	# Gets and stores all the ppv movies here.  Also deletes it from 
	# the movies hash if needed.  {{{
	for my $id ( keys %{ $movies } ) {
		if ( $movies->{ $id }->{is_ppv} ) {
			my %hash_t;
			my %hash = %{ $movies->{ $id } };
			my %hash_f = %{ $flag_h->{ $id } };
			if ( defined $m_thumbs->{ $id } ) {
				%hash_t = %{ $m_thumbs->{ $id } };
			}
			$ppvs_f->{ $id } = \%hash_f;
			$ppvs->{ $id } = \%hash;
			#jira ISD-117 assign the correct start_date for PPVs
			$ppvs->{ $id }->{sd} =  $href->{ $id }->{psd};
			if ( defined $m_thumbs->{ $id } ) {
				$p_thumbs->{ $id } = \%hash_t;
			}
			#Set to heydouga ppv site_id
			$ppvs->{ $id }->{s} = $site_ids->{3000};
			# Disable PPVs if price is 0
			if (!defined $ppvs->{ $id }->{pr} || $ppvs->{ $id }->{pr} == 0 ) {
				$ppvs_f->{ $id }->{en} = 0;
				$ppvs->{ $id }->{en} = 0;
				log_out( LOG_NOTICE, "PPV $id is disabled because the price is 0");
			}
			# jira ISD-117
			# if this PPV is not also monthly, delete from movies.
			# We don't assign ecp_start_date for linked movies 
			# anymore.
			if ( !$movies->{ $id }->{is_monthly} ) {
				delete( $movies->{ $id } );
				delete( $flag_h->{ $id } );
				delete( $m_thumbs->{ $id } );
			}
		}
	}
	undef $href;
	# }}}

	# If there is no movie, no need to proceed
	if ( keys( %{ $movies } ) > 0 ) {
		log_out( LOG_INFO, keys( %{ $movies } )." movies found, adding details.. " );
		add_movie_details( $movies, 'movies', $flag_h );
		add_flags( $movies, $flag_h, 'movies' );
		$pages =	get_movie_pages( $movies, 'movie_pages' );
		my $mpages;
		for my $seq ( keys %{ $pages } ) {
			for my $type ( keys %{ $pages->{ $seq } } ) {
				++$mpages;
			}
		}
		log_out( LOG_INFO, "$mpages movie pages added" );
	} else { 
		log_out( LOG_WARN, "No new movies to import for full" );
		undef( $movies );
	}
	# If there is no ppvs, no need to proceed
	if ( keys( %{ $ppvs } ) > 0 ) {
		log_out( LOG_INFO, keys( %{ $ppvs } )." ppvs found, adding details.. " );
		add_movie_details( $ppvs, 'ppvs', $ppvs_f );
		add_flags( $ppvs, $ppvs_f, 'ppvs' );
		$ppv_pages = 	get_movie_pages( $ppvs, 'ppv_pages' );
		my $mpages;
		for my $seq ( keys %{ $ppv_pages } ) {
			for my $type ( keys %{ $ppv_pages->{ $seq } } ) {
				++$mpages;
			}
		}
		log_out( LOG_INFO, "$mpages ppv pages added" );
	} else { 
		log_out( LOG_WARN, "No new ppvs to import for full" );
		undef( $ppvs );
	}
}
# }}}

# sub add_movie_details {{{
sub add_movie_details {
	my ( $href, $table, $href_f, $url, $path, $domain, $sub_domain ) = @_;
	log_out( LOG_INFO, "adding $table detail.." );
	for my $id ( keys %{ $href } ) {
		my $data = $href->{ $id };
		my $data_f = $href_f->{ $id };
		# Sample URL {{{
		if ( $data->{is_ppv} && $table eq 'ppvs' ) {
 			if ( defined $urls->{ppv} ) {
				$path = sprintf( $urls->{ppv}, $data->{org}, $data->{m} );
			}
			if ( defined $urls->{domain}->{2486}->{guest} ) {
				$domain = $urls->{domain}->{2486}->{guest};
			} 
		} elsif ( $table eq 'movies' ) {
			if ( defined $urls->{movie} ) {
				$path = sprintf( $urls->{movie}, $data->{m} );
			}
			if ( defined $urls->{domain}->{ $data->{s} }->{guest} ) {
				$domain     = $urls->{domain}->{ $data->{s} }->{guest};
			} else {
				log_out( LOG_WARN, "No guest domain defined in config: site_id $data->{s}" );
				delete( $href->{ $id } );
			}
		}
		if ( defined $href->{ $id } && defined $domain && defined $path ) {
			$data->{su} = $domain.$path;
			$data->{ls} = length( $data->{su} );
		} else { 
			$data->{su} = '';
			$data->{ls} = 0;
		}
		# }}}
		# vip/svip {{{
		if ( defined $tar_flag->{ $id } ) {
			my $tflag;
			if ( $table eq 'ppvs' ) {
				$tflag = $tar_flag->{ $id }->{2};
			} elsif ( $table eq 'movies' ) {
				if ( defined $tar_flag->{ $id }->{1} ) {
					$tflag = $tar_flag->{ $id }->{1};
				} elsif ( defined $tar_flag->{ $id }->{3} ) {
					$tflag = $tar_flag->{ $id }->{3};
				}
			}
			#my $tflag = $tar_flag->{ $id };
			if ( defined $tflag->{v} && defined $tflag->{sv} ) {
				if ( $tflag->{v} > 1 ) {
					log_out( LOG_WARN, "Unknown vip_flag value found (tar_flag): $tflag->{v} (movie_seq $id)" );
					delete( $href->{ $id } );
				} elsif ( $tflag->{sv} > 1 ) {
					log_out( LOG_WARN, "Unknown supervip_flag value found (tar_flag): $tflag->{sv} (movie_seq $id)" );
					delete( $href->{ $id } );
				} elsif ( $tflag->{v} == 1 ) {
					$data_f->{vip} = 1;
				} elsif ( $tflag->{sv} == 1 ) {
					$data_f->{svip} = 1;
				}
			}

		} else {
			# This movie is not ECP
			delete( $href->{ $id } );
		}
		# }}}


		my @ocids = ();
		# Save original categories
		if ( defined @{ $ecp_tag->{ $id } } ) {
			@ocids  = @{ $ecp_tag->{ $id } };
		}

		# Check categories for any duplicate values and set category count
		my $uc_count = @ocids;
		if ( $uc_count > 0 ) {
			my %count;
			@ocids = grep{ !$count{ $_ }++ } @ocids;
			@{ $data->{ocid} } = @ocids;
			$uc_count    = @ocids;
		}
		$data->{cocid} = $uc_count;

		# Save series ids {{{
		my @series_ids = ();
		if ( defined @{ $series_h->{ $id } } ) {
			@series_ids  = @{ $series_h->{ $id } };
		}

		# Check for duplicate values and set series id count
		my $series_count = @series_ids;
		if ( $series_count > 0 ) {
			my %count;
			@series_ids = grep{ !$count{ $_ }++ } @series_ids;
			@{ $data->{series_ids} } = @series_ids;
			$series_count = @series_ids;
		}
		$data->{cseriesid} = $series_count;
		# }}}

		# No actor ids for ecp
		$data->{cactorids} = 0;

		# Total Length for file bytes
		if ( defined $href->{ $id } ) {
			if ( $table eq 'movies') {
				$data->{tl} = ( $data->{la} + $data->{li} + $data->{ld} + $data->{ls} + ( 4 * $data->{cocid} ) + ( 4 * $data->{cseriesid} ) + ( 4 * $data->{cactorids} ) + $data->{ldr} + $data->{llorigin} + $data->{lloriginal} + $data->{lmvid} + $binary->{ $table }->{bytes} );
			} else {
				$data->{tl} = ( $data->{la} + $data->{li} + $data->{ld} + $data->{ls} + ( 4 * $data->{cocid} ) + ( 4 * $data->{cseriesid} ) + ( 4 * $data->{cactorids} ) + $data->{ldr} + $data->{lorg} + $data->{lpr} + $data->{llorigin} + $data->{lloriginal} + $data->{lmvid} + $binary->{ $table }->{bytes} );
			}
		}
	}
}
# }}}

# sub add_flags {{{
sub add_flags {
	my ( $href, $flag_h, $type ) = @_;
	my $FLAGS = $binary->{ $type }->{flags};

	if ( $type eq 'movie_pages' || $type eq 'ppv_pages' ) {
		for my $id ( keys %{ $flag_h } ) {
			for my $path ( keys %{ $flag_h->{ $id } } ) {
				my $data = $flag_h->{ $id }->{ $path };                                       
				my $flag_val = 0;                                                             
				for my $flag ( keys %{ $FLAGS } ) {
					if ( defined $data->{ $FLAGS->{ $flag } } && $data->{ $FLAGS->{ $flag } } eq 1 ) {
						$flag_val |= 1 << $flag_val;
					}
				}
				$href->{ $id }->{ $path }->{en} = $flag_val;
			}
		}
	} else {
		for my $id ( keys %{ $flag_h } ) {
			my $data     = $flag_h->{ $id };
			my $flag_val = 0;
			for my $flag ( keys %{ $FLAGS } ) {
				if ( defined $data->{ $FLAGS->{ $flag } } && $data->{ $FLAGS->{ $flag } } eq 1 ) {
					$flag_val |= 1 << $flag;
				}
			}
			$href->{ $id }->{f} = $flag_val;
		}
	}
	undef $flag_h;
}
# }}}

# sub get_movie_pages {{{
sub get_movie_pages {
	my ( $href, $table, $page_h, $flag ) = @_;
	if ( $table eq 'movie_pages' ) {
		log_out( LOG_INFO, "Getting pages.." );
		for my $id ( keys %{ $href } ) {
			my $data = $href->{ $id };
			while( my ( $type, $page_url ) = each( %{ $urls->{domain}->{ $data->{s} } } ) ) {
				my $domain = $urls->{domain}->{ $data->{s} }->{ $type };
				if ( defined $urls->{path} ) {
					my $m_path;
					if ( $data->{s} == 2585 || $data->{s} == 2555 || $data->{s} == 2592 || $data->{s} == 2554 ) {
						$m_path    = sprintf( $urls->{heydouga_path}, $data->{m} );

					} else {
						$m_path    = sprintf( $urls->{path}, $type, $data->{m} );
					}
					my $url       = sprintf( $urls->{movie}, $data->{m} );
					my %page_type = ( 'guest' => 1, 'regular' => 2, 'vip' => 3, 'svip' => 4 );
					my $type_map = $page_type{ $type };

					# For creamlemon and uramovie check page type and vip/svip flags match
					if ( $data->{s} == 2516 || $data->{s} == 2518 ) {
						# Get tar_flag record for this product
						my $tflag;
						if ( defined $tar_flag->{ $id } ) {
							if ( defined $tar_flag->{ $id }->{1} ) {
								$tflag = $tar_flag->{ $id }->{1};
							} elsif ( defined $tar_flag->{ $id }->{3} ) {
								$tflag = $tar_flag->{ $id }->{3};
							}
						}
						if ( ! defined $tflag ) {
							# No record in tar_flags for this product
							log_out( LOG_WARN, "No record in tar_flag for movie_seq $id, type $type" );
							next;
						}

						# Clear has_sample flag for movie/ppv if vip or svip
						my $flag_h = $binary->{'movies'}->{flags};
						my $flag_val = 0;
						for my $flag ( keys %{ $flag_h } ) {
							if ( $flag_h->{$flag} eq 'hs' ) {
								$flag_val = 1 << $flag;
								last;
							}
						}

						if ( $type eq 'vip' ) {
							next if ( $tflag->{v} != 1 );
							if ( $data->{f} & $flag_val ) {
								$data->{f} ^= $flag_val;
							}
						}
						elsif ( $type eq 'svip' ) {
							next if ( $tflag->{sv} != 1 );
							if ( $data->{f} & $flag_val ) {
								$data->{f} ^= $flag_val;
							}
						}
						else {
							next if ( $tflag->{v} == 1 || $tflag->{sv} == 1 );
						}
					}

					$page_h->{ $id }->{ $type_map }->{path} = $m_path;
					$page_h->{ $id }->{ $type_map }->{lp} = length( $m_path );
					$page_h->{ $id }->{ $type_map }->{t}  = $type_map;
					$page_h->{ $id }->{ $type_map }->{u}  = $domain.$url;
					$page_h->{ $id }->{ $type_map }->{lu} = length( $page_h->{ $id }->{ $type_map }->{u} );
					$page_h->{ $id }->{ $type_map }->{tl} = ( $page_h->{ $id }->{ $type_map }->{lp} + $page_h->{ $id }->{ $type_map }->{lu} + $binary->{ $table }->{bytes} );
					# Flag - is_enabled
					# Set this to false if the movie itself is disabled
					$page_h->{ $id }->{ $type_map }->{en} = ( $data->{en} == 1 ) ? 1 : 0;
				}
			}
		}
		add_flags( $page_h, $flag, $table );
	} else {
		log_out( LOG_INFO, "Getting pages.." );
		for my $id ( keys %{ $href } ) {
			my $data = $href->{ $id };
			while( my ( $type, $page_url ) = each( %{ $urls->{domain}->{2486} } ) ) {
				my $domain = $urls->{domain}->{2486}->{ $type };
				if ( defined $urls->{path} ) {
					my $m_path    = sprintf( $urls->{ppv_heydouga_path}, $data->{org}, $data->{m} );
					my $url       = sprintf( $urls->{ppv}, $data->{org}, $data->{m} );
					my %page_type = ( 'guest' => 1, 'regular' => 2, 'vip' => 3, 'svip' => 4 );
					my $type_map = $page_type{ $type };
					$page_h->{ $id }->{ $type_map }->{path} = $m_path;
					$page_h->{ $id }->{ $type_map }->{lp} = length( $m_path );
					$page_h->{ $id }->{ $type_map }->{t}  = $type_map;
					$page_h->{ $id }->{ $type_map }->{u}  = $domain.$url;
					$page_h->{ $id }->{ $type_map }->{lu} = length( $page_h->{ $id }->{ $type_map }->{u} );
					$page_h->{ $id }->{ $type_map }->{tl} = ( $page_h->{ $id }->{ $type_map }->{lp} + $page_h->{ $id }->{ $type_map }->{lu} + $binary->{ $table }->{bytes} );
					$page_h->{ $id }->{ $type_map }->{en} = ( $data->{en} == 1 ) ? 1 : 0;
				}
			}
		}
		add_flags( $page_h, $flag, $table );
	}
	return $page_h;
}
# }}}

# sub get_files {{{
sub get_files {
	my ( $table, $sql, $href, $flag_h, $url ) = shift @_;
	log_out( LOG_INFO, "Getting movie_files.. " );
	$sql = $SQL.$table.";";
	my $sth = $dbi->prepare( $sql );
	$sth->execute();
	$href  = $dbi->selectall_hashref( $sth, 'file_number' );

	if ( keys( %{ $href } ) < 1 ) {
		log_out( LOG_WARN, "No movie files to import for full" );
		return;
	}

	for my $fid ( keys %{ $href } ) {
		my $data = $href->{ $fid };
		my $seq  = $href->{ $fid }->{movie_seq};

		# Basic check and data conversion {{{		
		# If any important field has missing value, complain and skip it
		while ( my ( $key, $val ) = each %{ $data } ) {
			if ( $key eq 'status' && ( !defined $val || $val < 0 || $val > 2 ) ) {
				log_out( LOG_WARN, "Invalid movie file status found: $val (file_number $fid, movie_seq $seq" );
				delete( $href->{ $fid } );
			} elsif ( $key eq 'status' || $key eq 'file_size' || $key eq 'server_name' || $key eq 'path' || $key eq 'file_type' || $key eq 'sample_flag' ) {
				if ( !defined $val || $val eq '' ) {
					if ( $data->{status} == 1 ) {
						log_out( LOG_WARN, "Undefined value ($table): key - $key (file_number $fid, movie_seq $seq)" );
					} else {
						log_out( LOG_NOTICE, "Undefined value ($table): key - $key (file_number $fid, movie_seq $seq)" );
					}
					delete( $href->{ $fid } );
				}
			}
			if ( $key eq 'file_size' || $key eq 'bitrate'	) {
				if ( $val < 0 ) {
					if ( $data->{status} == 1 ) {
						log_out( LOG_WARN, "Invalid $key ($table): $val (file_number $fid, movie_seq $seq)" );
					} else {
						log_out( LOG_NOTICE, "Invalid $key ($table): $val (file_number $fid, movie_seq $seq)" );
					}
					delete( $href->{ $fid } );
				}
			}
			if ( $key eq 'file_type' ) {
				if ( $val ne 'zip' && ( $data->{codec} eq '' || !defined $data->{codec} ) ) {
					if ( $data->{status} == 1 ) {
						log_out( LOG_WARN, "No codec defined ($table): file_number $fid, movie_seq $seq" );
						delete( $href->{ $fid } );
					} else {
						log_out( LOG_NOTICE, "No codec defined ($table): file_number $fid, movie_seq $seq" );
						delete( $href->{ $fid } );
					}
				}
				if ( $data->{externalsite_flag} == 1 || $val eq 'streaming' || $val eq 'streaming/ipod' ) {
					if ( $data->{bitrate} == 0 || !defined $data->{bitrate} ) {
						log_out( LOG_WARN, "bitrate undefined ($table): file_number $fid, movie_seq $seq" ) if ( $data->{status} == 1 );
						log_out( LOG_NOTICE, "bitrate undefined ($table): file_number $fid, movie_seq $seq" ) if ( $data->{status} != 1 );
						delete( $href->{ $fid } );
					} elsif ( $data->{file_size} == 0 || !defined $data->{file_size} ) {
						log_out( LOG_WARN, "file_size undefined ($table): file_number $fid, movie_seq $seq" ) if ( $data->{status} == 1 );
						log_out( LOG_NOTICE, "file_size undefined ($table): file_number $fid, movie_seq $seq" ) if ( $data->{status} != 1 );
						delete( $href->{ $fid } );
					} else {
						if ( defined $href->{ $fid } ) {
							$data->{m} = sprintf( "%.u", ( 30 * $data->{bitrate} / 8 ) );
							if ( $data->{sample_flag} != 1 && $data->{m} > $data->{file_size} ) {
								log_out( LOG_WARN, "Invalid min_size ($table): ($data->{m} > $data->{file_size}) file_number $fid, movie_seq $seq" ) if ( $data->{status} == 1 );
								log_out( LOG_NOTICE, "Invalid min_size ($table): ($data->{m} > $data->{file_size}) file_number $fid, movie_seq $seq" ) if ( $data->{status} != 1 );
								delete( $href->{ $fid } );
							}
						}
					}
					$data->{t} = ( $data->{externalsite_flag} == 1 ) ? 3 : 1 if ( defined $href->{ $fid } );
				} elsif ( $val eq 'download' || $val eq 'iphone' ) {
					$data->{t} = 2 if ( defined $href->{ $fid } );
				} elsif ( $val eq 'zip' ) {
					# We don't import zip files since it's a gallery not a movie
					delete( $href->{ $fid } );
				} else {
					# Unknown file type
					log_out( LOG_WARN, "Unknown file type found ($table): $val (file_number $fid, movie_seq $seq)" );
					delete( $href->{ $fid } );
				}
			}
		}
		# }}}
	}
	my $ppv_flags;
	my $movie_flags;

	for my $fid ( keys %{ $href } ) {
		my $data = $href->{ $fid };
		my $seq  = $href->{ $fid }->{movie_seq};
		my $url  = 'http://'.$data->{server_name}.$data->{path};
		my $product = \%{ $movies };
		my $file_ref = \%{ $files };
		my $flag_ref;
		my $section_bytes = $binary->{movie_files}->{bytes};
		#if it's ppv, set reference to ppv hashes
		if ( $data->{product_type} == 2 && $ppvs->{ $seq } && $ppvs->{ $seq }->{is_ppv} ) {
			$product = \%{$ppvs};
			$file_ref = \%{$ppv_files};
			$flag_ref = \%{ $ppv_flags };
			$section_bytes = $binary->{ppv_files}->{bytes};
		} else {
			$flag_ref = \%{ $movie_flags };
		} 

		# movie files {{{
		if ( exists $product->{ $seq } ) {
			$file_ref->{ $fid }->{m} = ( defined $data->{m} ) ? $data->{m} : 0;
			$file_ref->{ $fid }->{p}  = $seq;
			$file_ref->{ $fid }->{t}  = $data->{t};
			$file_ref->{ $fid }->{s}  = ( defined $data->{file_size} ) ? $data->{file_size} : 0;
			$file_ref->{ $fid }->{b}  = ( defined $data->{bitrate} ) ? $data->{bitrate} : 0;
			$file_ref->{ $fid }->{u}  = $url;
			$file_ref->{ $fid }->{c}  = $data->{codec};
			$file_ref->{ $fid }->{pa} = $data->{path};
			$file_ref->{ $fid }->{lu} = length( $url );
			$file_ref->{ $fid }->{lc} = length( $data->{codec} );
			$file_ref->{ $fid }->{lp} = length( $data->{path} );
			$file_ref->{ $fid }->{tl} = ( $file_ref->{ $fid }->{lu} + $file_ref->{ $fid }->{lc} + $file_ref->{ $fid }->{lp} + $section_bytes );
			$flag_ref->{ $fid }->{en}    = ( $data->{status} == 1 ) ? 1 : 0;
			$flag_ref->{ $fid }->{is}    = ( $data->{sample_flag} == 1 ) ? 1 : 0;
		# }}}
		}
	}

	if ( keys( %{ $files } ) > 0 ) {
		add_flags( $files, $movie_flags, 'movie_files' );
		log_out( LOG_INFO, keys( %{ $files} )." movie files to import" );
	} else {
		log_out( LOG_WARN, "No movie files to import for full" );
	}
	if ( keys( %{ $ppv_files } ) > 0 ) {
		add_flags( $ppv_files, $ppv_flags, 'ppv_files' );
		log_out( LOG_INFO, keys( %{ $ppv_files} )." ppv files to import" );
	} else {
		log_out( LOG_WARN, "No ppv files to import for full" );
	}
	log_out( LOG_INFO, "movie_file import end." );
}
# }}}

# sub bytes {{{
sub bytes { 
	my ( $href, $table, $item_count, $section_bytes ) = @_; 
	log_out( LOG_INFO, "Generating bytes for $table.." ); 

	# movies, movie_files {{{
	if ( $table eq 'movies' || $table eq 'movie_files' ) {
		$item_count = keys( %{ $href } );
		if ( $item_count > 0 ) {
			for my $id ( keys %{ $href } ) {
				$section_bytes += $href->{ $id }->{tl};
			}
			$BYTES->{ $table }->{ic} = $item_count;
			$BYTES->{ $table }->{sb} = $section_bytes;
		}
	# }}}
	# movie_pages {{{
	} elsif ( $table eq 'movie_pages' || $table eq 'ppv_pages' ) {
		$item_count = 0;
		for my $id ( keys %{ $href } ) {
			for my $path ( keys %{ $href->{ $id } } ) {
				$item_count++;
				if ( $item_count > 0 ) {
					$section_bytes += $href->{ $id }->{ $path }->{tl};
				}
			}
			$BYTES->{ $table }->{ic} = $item_count;
			$BYTES->{ $table }->{sb} = $section_bytes;
		}
	# }}}
	# ppvs, movie_files {{{
	} elsif ( $table eq 'ppvs' || $table eq 'ppv_files' ) {
		$item_count = keys( %{ $href } );
		if ( $item_count > 0 ) {
			for my $id ( keys %{ $href } ) {
					$section_bytes += $href->{ $id }->{tl};
			}
			$BYTES->{ $table }->{ic} = $item_count;
			$BYTES->{ $table }->{sb} = $section_bytes;
		}
	# }}}
	# movie_thumbs, ppv_thumbs {{{
	} elsif ( $table eq 'movie_thumbs' || $table eq 'ppv_thumbs' ) {
		for my $id ( keys %{ $href } ) {
			for my $type ( keys %{ $href->{ $id } } ) {
				$item_count++;
				if ( $item_count > 0 ) {
					$section_bytes += $href->{ $id }->{ $type }->{tl};
				}
			}
			$BYTES->{ $table }->{ic} = $item_count;
			$BYTES->{ $table }->{sb} = $section_bytes;
		}
	# }}}
	}
}
# }}}

# sub create file {{{
sub create_file {
	my ( $total_sec_bytes );
	my $out = $file.time();
	my $tmp = $out.".tmp";
	log_out( LOG_INFO, "Creating binary file.." );

	# Calculate bytes of the whole file
	my $sec_count = keys( %{ $BYTES } );
	for my $table ( keys %{ $BYTES } ) {
		$total_sec_bytes += $BYTES->{ $table }->{sb};
	}
	my $file_bytes = $total_sec_bytes + ( $config->{section} * $sec_count );

	# File Header 
	$str = pack( "a4 S L L", "META", $origin_id, $sec_count, $file_bytes );

	for my $table ( keys %{ $BYTES } ) {
		my $data = $BYTES->{ $table };
		# Version 2 - added movie_pages status support
		$str .= pack( "S C L L", $binary->{ $table }->{type}, 2, $data->{ic}, $data->{sb} );
		create_item( $table );
	}
	open( FILE, '>', $tmp ) || die log_out( LOG_WARN, "open( $tmp ): $!" );
	print FILE $str;
	close( FILE );
	rename( $tmp, $out ) || die log_out( LOG_WARN, "rename($tmp): $!" );
	log_out( LOG_INFO, "File $out created" );
}
# }}}

# sub create_item {{{
sub create_item { 
	my ( $table ) = shift; 
	log_out( LOG_INFO, "Creating section for $table.." );

	# Detail of meta binary format - http://wiki.ent/A/TAR/Design/MetaBinaryFormat
	# movies {{{  
	if ( $table eq 'movies' ) {
		for my $id ( keys %{ $movies } ) {
			my $data = $movies->{ $id };
			$str .= pack( "L L Q L S S S S S S S S S S S a$data->{li} a$data->{ls} a$data->{ld} a$data->{la}", $id, $data->{s}, $data->{sd}, $data->{f}, $data->{li}, $data->{ls}, $data->{ld}, $data->{cocid}, $data->{la}, $data->{ldr}, $data->{llorigin}, $data->{lloriginal}, $data->{lmvid}, $data->{cseriesid}, $data->{cactorids}, $data->{i}, $data->{su}, $data->{d}, $data->{an} );
			if ( $data->{cocid} > 0 ) {
				for my $uc_id ( @{ $data->{ocid} } ) {
					$str .= pack( "L", $uc_id );
				}
			}
			$str .= pack( "a$data->{ldr} a$data->{llorigin} a$data->{lloriginal} a$data->{lmvid}", $data->{dr}, $data->{lorigin}, $data->{loriginal}, $data->{m} );
			if ( $data->{cseriesid} > 0 ) {
				for my $series_id ( @{ $data->{series_ids} } ) {
					$str .= pack( "L", $series_id );
				}
			}
			if ( $data->{cactorids} > 0 ) {
				for my $actor_id ( @{ $data->{actor_ids} } ) {
					$str .= pack( "L", $actor_id );
				}
			}
		}
	}
	# }}}
	# movie_files {{{
	if ( $table eq 'movie_files' ) {
		for my $id ( keys %{ $files } ) {
			my $data = $files->{ $id };
			$str .= pack( "L C L Q Q Q S S S a$data->{lu} a$data->{lc} a$data->{lp}", $data->{p}, $data->{t}, $data->{f}, $data->{s}, $data->{m}, $data->{b}, $data->{lu}, $data->{lc}, $data->{lp}, $data->{u}, $data->{c}, $data->{pa} );
		}
	}
	# }}}
	# movie_pages {{{
	if ( $table eq 'movie_pages' ) {
		for my $id ( keys %{ $pages } ) {
			for my $path ( keys %{ $pages->{ $id } } ) {
				my $data = $pages->{ $id }->{ $path };
				$str .= pack( "L C L S S a$data->{lp} a$data->{lu}", $id, $data->{t}, $data->{en}, $data->{lp}, $data->{lu}, $data->{path}, $data->{u} );
			}
		}
	}
	# }}}
	# ppvs {{{  
	if ( $table eq 'ppvs' ) {
		for my $id ( keys %{ $ppvs } ) {
			my $data = $ppvs->{ $id };
			$str .= pack( "L L Q L S S S S S S S S S S S S S a$data->{li} a$data->{ls} a$data->{ld} a$data->{la}", $id, $data->{s}, $data->{sd}, $data->{f}, $data->{li}, $data->{ls}, $data->{ld}, $data->{cocid}, $data->{la}, $data->{lpr}, $data->{ldr}, $data->{lorg}, $data->{llorigin}, $data->{lloriginal}, $data->{lmvid}, $data->{cseriesid}, $data->{cactorids}, $data->{i}, $data->{su}, $data->{d}, $data->{an} );
			if ( $data->{cocid} > 0 ) {
				for my $uc_id ( @{ $data->{ocid} } ) {
					$str .= pack( "L", $uc_id );
				}
			}
			$str .= pack( "a$data->{lpr} a$data->{ldr} a$data->{lorg} a$data->{llorigin} a$data->{lloriginal} a$data->{lmvid}", $data->{pr}, $data->{dr}, $data->{meta_org}, $data->{lorigin}, $data->{loriginal}, $data->{m} );
			if ( $data->{cseriesid} > 0 ) {
				for my $series_id ( @{ $data->{series_ids} } ) {
					$str .= pack( "L", $series_id );
				}
			}
			if ( $data->{cactorids} > 0 ) {
				for my $actor_id ( @{ $data->{actor_ids} } ) {
					$str .= pack( "L", $actor_id );
				}
			}
		}
	}
	# }}}
	# ppv_files {{{
	if ( $table eq 'ppv_files' ) {
		for my $id ( keys %{ $ppv_files } ) {
			my $data = $ppv_files->{ $id };
			$str .= pack( "L C L Q S S S a$data->{lu} a$data->{lc} a$data->{lp}", $data->{p}, $data->{t}, $data->{f}, $data->{s}, $data->{lu}, $data->{lc}, $data->{lp}, $data->{u}, $data->{c}, $data->{pa} );
		}
	}
	# }}}
	# ppv_pages {{{
	if ( $table eq 'ppv_pages' ) {
		for my $id ( keys %{ $ppv_pages } ) {
			for my $path ( keys %{ $ppv_pages->{ $id } } ) {
				my $data = $ppv_pages->{ $id }->{ $path };
				$str .= pack( "L C L S S a$data->{lp} a$data->{lu}", $id, $data->{t}, $data->{en}, $data->{lp}, $data->{lu}, $data->{path}, $data->{u} );
			}
		}
	}
	# }}}
	# movie_thumbs {{{
	if ( $table eq 'movie_thumbs' ) {
		for my $id ( keys %{ $m_thumbs } ) {
			for my $type ( keys %{ $m_thumbs->{ $id } } ) {
				my $data = $m_thumbs->{ $id }->{ $type };
				$str .= pack( "L S S S S L S S a$data->{l_url} a$data->{l_md5}", $id, $type, $data->{level}, $data->{order}, $data->{size}, $data->{flags}, $data->{l_url}, $data->{l_md5}, $data->{url}, $data->{md5} );
			}
		}
	}
	# ppv_thumbs {{{
	if ( $table eq 'ppv_thumbs' ) {
		for my $id ( keys %{ $p_thumbs } ) {
			for my $type ( keys %{ $p_thumbs->{ $id } } ) {
				my $data = $p_thumbs->{ $id }->{ $type };
				$str .= pack( "L S S S S L S S a$data->{l_url} a$data->{l_md5}", $id, $type, $data->{level}, $data->{order}, $data->{size}, $data->{flags}, $data->{l_url}, $data->{l_md5}, $data->{url}, $data->{md5} );
			}
		}
	}
}
# }}}

# sub valid_date {{{ 
sub valid_date { 
	my ( $href, $seq, $date ) = @_;
	my ( $year, $month, $day ) = ( substr( $date, 0, 4 ), substr( $date, 5, 2 ), substr( $date, 8, 2 ) );
	my $lastday = ( 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ) [ $month - 1 ];

	if ( $month == 2 ) { 
		++$lastday if ( ( ( $year % 4 == 0 ) && ( $year % 100 != 0 ) ) || ( $year % 400 == 0 ) );
	}
	if ( $day > $lastday ) {
		log_out( LOG_WARN, "Invalid date found - $date (movie_seq $seq)" );
		delete $href->{ $seq };
	}
}
# }}}

# sub convert {{{
sub convert {
	my ( $local, $y, $m, $d, $h, $mi, $s ) = shift;
	( $y, $m, $d )  = split( /-/, $local );
	( $h, $mi, $s ) = qw( 00 00 00 );
	my $epoch = timegm( $s, $mi, $h, $d, ( $m - 1 ), $y );
	return $epoch;
}
# }}}
