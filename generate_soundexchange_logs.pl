#!/usr/bin/perl

## Script to generate SoundExchange logs for submission to NPR
# Author: Kit Peters <cpeters@ucmo.edu>
# Date: 10 January 2013
# Playlist log generation based on code by David Houghton.
#
# Notes:
# This is written for use at KTBG.  In particular, it assumes database access via ODBC with the field names that KTBG uses, Windows Media Services logging,
# and Simian playlists.  For other stations, modify generate_playlist_logs() and/or generate_streaming_logs() (or the appropriate helper function(s))
# accordingly.
use strict;
use warnings;

use autodie qw(:io);
use feature qw(say switch);
use English qw/-no_match_vars/;
use Text::CSV_XS;
use Readonly;
use Time::Piece;
use DBI;
use Storable qw/dclone/;
use File::Spec;
use File::Spec::Win32; # needed b/c file paths stored in DB are Win32 paths
use File::stat;
use Carp qw/carp croak/;
use IPC::Open3 qw/open3/;
use Sys::Hostname qw/hostname/;
use Cwd qw/getcwd abs_path/;
use Net::SMTP;
use Email::MIME::Creator;
use IO::All;
use Archive::Zip qw/AZ_OK/;
use File::Temp qw/tempfile/;

our $VERSION = q/2.2/;

# Global variables section - do not modify these
Readonly my $YEAR         => localtime->year;
Readonly my $LAST_QUARTER => _get_last_quarter();

# Field names for Simian playlist file
Readonly my @PLAYLIST_FIELD_NAMES => qw/played actual ignore1 index ignore2 scheduled cut_id track_type track_name ignore3 file_name/;
Readonly my $EMPTY                => q{};
Readonly my $SECONDS_PER_MINUTE   => 60;
Readonly my $BITS_PER_BYTE        => 8;

# Configuration section
Readonly my $CALL_LETTERS => q/KTBG/;    # Station call letters
Readonly my %CONFIG => (
# sample rate for music files, in Hz
    q/sample_rate/                  => 44_100,

# bits per sample for music files
    q/bits_per_sample/              => 16,

# number of channels for music files
    q/channels/                     => 2,

# station call letters.  Change above as this is referenced below.
    q/call_letters/                 => $CALL_LETTERS,

# database where song information is stored
    q/database_name/                => q/ktbgfm/,

# field names for playlist SX log
    q/playlist_output_field_names/  => [ q/Start Time/, q/Duration/, q/Artist/, q/Title/, q/Album/, q/Label/ ],

# field names for streaming SX log
    q/streaming_output_field_names/ => [ q/IP Address/, q/Date/, q/Time/, q/Stream name/, q/Duration/, q/Status Code/, q/Referrer/ ],

# Separator character for WMS log files.
    q/input_delimiter/              => q/ /,

# Field delimiter for SX log files
    q/output_delimiter/             => qq/\t/,

# Output record separator
    q/output_record_separator/      => qq/\n/,

# Number of consecutive days for which we need data
    q/days_needed/                  => 14,

# Output file prefix for stream logs
    q/stream_log_file_prefix/       => qq/$CALL_LETTERS $LAST_QUARTER $YEAR Stream Log - /,

# Output file name for playlist logs
    q/playlist_log_file_name/       => qq/$CALL_LETTERS $LAST_QUARTER $YEAR Playlist Log.txt/,

# Directory in which to look for streaming log files.  Subdirectories will be treated as stream names.
    q/stream_logs_dir/              => q|/mnt/stream_logs/WMS|,

# Directory in which to look for playlist log files.
    q/playlist_logs_dir/            => q|/mnt/60gig/Logs|,

# Directory in which music files are stored (for duration calculation in playlist SX log generation)
    q/music_dir/                    => q|/mnt/100gig/A3|,

# SMTP host for notification emails
    q/smtp_host/                    => q|153.91.42.35|,

# "From" address for notification emails
    q/notify_from/                  => q|noreply@ktbg.int|,

# "To" address for notification emails
#    q/notify_to/                    => q|bjohnson@ktbg.fm|,
    q/notify_to/                    => q|cpeters@ucmo.edu|,

# DSN of database to connect to
    q/dsn/                          => q{ktbg_local},
    q/compilations_data_file/       => q{compilations.csv},
    q/use_csv/                      => 1,
);

main();

sub main {
    my $log_output_dir = File::Temp::tempdir();

    my @parsed_logs = eval { generate_streaming_logs($log_output_dir); } or croak(qq/Failed to generate streaming log: $EVAL_ERROR/);
    my %log_dates;

    # Streaming logs are of the form WMS_YYYYMMDD(_NNN).log, while Simian logs are of the form YYMMDD.lst.
    # Convert the YYYYMMDD portion of the former to the YYMMDD for the latter
    for my $filename (@parsed_logs) {
        if ( $filename !~ /^WMS.+[.]log/xsm ) {
            next;
        }
        ( my $date = $filename ) =~ s/^WMS_\d{2}(\d{6}).+/$1/xsm;
        $log_dates{$date} = 1;
    }
    eval { 
        generate_playlist_logs( $log_output_dir, [sort keys %log_dates] ); 
        1;
    } or croak(qq/Failed to generate playlist log: $EVAL_ERROR/);

    eval {
        email_logs($log_output_dir);
    } or croak(qq/Failed to email logs: $EVAL_ERROR/);

    return 1;
}

# parse the Simian playlist files for the supplied dates (in YYMMDD format), pull the requisite information from the database,
# and write the Sound_exchange playlist log for that date range.
sub generate_playlist_logs {
    my ($log_output_dir, $log_dates) = @_;
    if ( scalar @{$log_dates} == 0 ) {
        croak(q/No log dates supplied/);
    }
    my $output_file = File::Spec->catfile( $log_output_dir, $CONFIG{'playlist_log_file_name'} );

    open my $output_fh, q/>:encoding(UTF8)/, $output_file;
    _print_playlist_log( $output_fh, $log_dates );
    close $output_fh;

    return 1;    # return a true value so eval() is satisfied
}

# Parse the WMS streaming logs for the last quarter, write the Sound_exchange log files for each stream, and return a list of the log files parsed.
sub generate_streaming_logs {
    my $output_dir = shift;
    if ( !-e $CONFIG{'stream_logs_dir'} ) {
        croak(qq/Stream log directory "$CONFIG{'stream_logs_dir'}" could not be found/);
    }
    if ( !-d $CONFIG{'stream_logs_dir'} ) {
        croak(qq/"$CONFIG{'stream_logs_dir'}" is not a directory/);
    }
    my @streams = _list_dir( $CONFIG{'stream_logs_dir'} );
    my @daily_logs;    # KLUDGE: I need to have some way to get the dates of the log files found.
    for my $stream_name (@streams) {
        if ( $stream_name eq '[Global]' ) {    # skip this one; it contains records for all streams
            next;
        }
        @daily_logs = _print_stream_log($output_dir, $stream_name);
    }
    return @daily_logs;
}

# helper function for generate_playlist_logs
sub _print_playlist_log {
    my $fh        = shift;
    my $log_dates = shift;

    my $csv = Text::CSV_XS->new( { q/sep_char/ => $CONFIG{'output_delimiter'}, eol => $CONFIG{'output_record_separator'} } );

    $csv->print( $fh, \@{ $CONFIG{'playlist_output_field_names'} } );
    for my $log_date ( @{$log_dates} ) {
        my $full_date = Time::Piece->strptime( $log_date, q/%y%m%d/ )->strftime(q|%m/%d/%Y|);
        my $playlist = File::Spec->catfile( $CONFIG{'playlist_logs_dir'}, qq/$log_date.lst/ );
        
        my @songs;
        if ($CONFIG{'use_csv'}) {
            @songs = _get_song_records_csv($playlist);
        }
        else {
            @songs = _get_song_records_db($playlist);
        }
        for my $song (@songs) {
            $csv->print( $fh, [ qq/$full_date $song->{'actual'}/, $song->{'duration'}, $song->{'Artist_Name'}, $song->{'Song_Name'}, $song->{'Album_Name'}, $song->{'Record_Company'} ] );
        }
    }
    return 1;
}

sub _get_song_records_csv {
    my $playlist = shift;
    my $compilations_data = _get_compilations_data();
    my @songs;
    eval {
        @songs = _parse_playlist($playlist);
        for my $song (@songs) {    # hey, it's a song of songs!
            _build_song_record_csv( $compilations_data, $song );
        }
        1;
    } or croak(qq/Failed to parse playlist $playlist: $EVAL_ERROR/);
    return @songs;
}

sub _get_song_records_db {
    my $playlist = shift;
    my @songs;
    
    my $connect_string = qq/DBI:ODBC:$CONFIG{'dsn'}/;
    my $dbh = DBI->connect( $connect_string, $EMPTY, $EMPTY, { q/PrintError/, => 1, q/RaiseError/ => 1, 'odbc_batch_size' => 1 } );
    eval {
        @songs = _parse_playlist($playlist);
        for my $song (@songs) {    # hey, it's a song of songs!
            _build_song_record_db( $dbh, $song );
        }
        1;
    } or croak(qq/Failed to parse playlist $playlist: $EVAL_ERROR/);
    return @songs;
}

# helper function for _print_playlist_log
sub _build_song_record_csv {
    my $compilations_data = shift;
    my $song = shift;
    my $song_record = $compilations_data->{$song->{'cut_id'}};

    @{$song}{keys %{$song_record}} = values %{$song_record};
        
    $song->{'duration'} = _get_duration( $song->{'file_name'} );
    for my $field (qw/Artist_Name Song_Name Album_Name Record_Company/) {
        $song->{$field} ||= 'N/A';                               # fill in blanks with "N/A"
    }
    return 1;
}

sub _build_song_record_db {
    my ($dbh, $song) = @_;
    my $sth = eval {
        $dbh->prepare(qq/SELECT Song_Name, Artist_Name, Album_Name, Year, Record_Company FROM compilations WHERE Song_ID = 'xxx$song->{'cut_id'}' /);
    };
    if ($EVAL_ERROR) {
        croak(qq/Failed to prepare song select statement: $EVAL_ERROR/);
    }
    if ($sth->errstr) { 
        croak(qq/Failed to prepare song select statement: $sth->errstr/);
    }
    # this doesn't work on Linux / mdbtools for some reason...
    $sth->bind_columns( \( @{$song}{ @{ $sth->{NAME} } } ) );    # this allows me to put all the song info into one hashref
    eval {
        $sth->fetch;
        1;
    } or croak(qq/Failed to fetch song data: $EVAL_ERROR/);

    $song->{'duration'} = _get_duration( $song->{'file_name'} );
    for my $field (qw/Artist_Name Song_Name Album_Name Record_Company/) {
        $song->{$field} ||= 'N/A';                               # fill in blanks with "N/A"
    }
}

# Calculate the approximate duration of an audio track from its size
sub _get_duration {
    my $file_name = shift;

    # extract the file name from the full path and file name
    my $basename  = [ File::Spec::Win32->splitpath($file_name) ]->[2]; 

    # use the file name relative to the specified music directory
    my $file                = File::Spec->catfile( $CONFIG{'music_dir'}, $basename );                                                   
    my $size                = stat($file)->size; # file size in bytes
    my $bytes_per_second    = $CONFIG{'sample_rate'} * $CONFIG{'channels'} * $CONFIG{'bits_per_sample'} / $BITS_PER_BYTE; 
    my $length              = $size / $bytes_per_second;
    my $minutes             = int( $length / $SECONDS_PER_MINUTE );
    my $seconds             = $length % $SECONDS_PER_MINUTE;
    return sprintf q/%02d:%02d/, $minutes, $seconds;
}

sub _print_stream_log {
    my ($output_dir, $stream_name) = @_;
    my $stream_subdir = File::Spec->catfile( $CONFIG{'stream_logs_dir'}, $stream_name );
    my $output_file   = File::Spec->catfile( $output_dir, qq/$CONFIG{'stream_log_file_prefix'} $stream_name.txt/ );

    my @daily_logs = _get_last_quarter_wmslogs( $stream_subdir, $CONFIG{'days_needed'} );
    my @parsed;
    for my $daily_log (@daily_logs) {
        my $stream_log_file = File::Spec->catfile( $stream_subdir, $daily_log );
        @parsed = ( @parsed, _parse_wmslog_file($stream_log_file) );
    }
    my $csv = Text::CSV_XS->new( { q/sep_char/ => $CONFIG{'output_delimiter'}, eol => $CONFIG{'output_record_separator'} } );

    open my $fh, q|>:encoding(UTF-8)|, qq/$output_file/;
    $csv->print( $fh, $CONFIG{'streaming_output_field_names'} );

    @parsed = reverse sort { $a->[1] cmp $b->[1] || $a->[2] cmp $b->[2] } @parsed;    # sort by date, then time
    for my $line (@parsed) {
        $csv->print( $fh, $line );
    }
    close $fh;
    return @daily_logs;
}

# take a Simian playlist file and return a list of songs played, including start time and cut ID
sub _parse_playlist {
    my $filename = shift or croak(q/Usage: parse_playlist(<filename>)/);
    my @songs;

    open my $fh, q/<:encoding(ISO-8859-1)/, $filename;
    @songs = _get_songs($fh);
    close $fh;
    return @songs;
}

# helper for parse_playlist
sub _get_songs {
    my $fh  = shift;
    my $csv = Text::CSV_XS->new(
        {
            'sep_char'           => q{|},
            'binary'             => 1,
            'allow_loose_quotes' => 1
        }
    );

    my @songs;
    my $row = {};
    $csv->bind_columns( \@{$row}{@PLAYLIST_FIELD_NAMES} );

    # Pull out the songs
    while ( my $line = <$fh> ) {
        $csv->parse($line) or croak( q/Unable to parse line: / . Text::CSV_XS->error_diag() );
        if ( $csv->parse($line) ) {

            # songs have a blank track_type field and a 7 alphanumeric character cut_id
            if ( $row->{'track_type'} eq $EMPTY && $row->{'cut_id'} =~ /^\w{7}$/xsm ) {
                my $copy = dclone($row);
                push @songs, $copy;
            }
        }
    }
    return @songs;
}

# parse the supplied WMS log file into a 2 dimensional array in the field order specified by NPR for 2012 SX reporting requirements
sub _parse_wmslog_file {
    my $logfile = shift;

    open my $fh, q/<:encoding(UTF-8)/, $logfile;
    my @parsed = eval { _parse_log_file($fh); };
    if ($EVAL_ERROR) {
        croak(qq/Failed to parse $logfile: $EVAL_ERROR/);
    }
    close $fh;

    return @parsed;
}

# helper function for _parse_wmslog_file
sub _parse_log_file {
    my $fh           = shift;
    my $csv          = Text::CSV_XS->new( { q/binary/ => 0, q/sep_char/ => $CONFIG{'input_delimiter'} } );
    my @column_names = _get_column_names($fh);
    my $row          = {};
    my @parsed;
    $csv->bind_columns( \@{$row}{@column_names} );
    while ( $csv->getline($fh) ) {
        (my $stream_name = lc $row->{'cs-uri-stem'}) =~ s/^\///;
        my $ip          = $row->{'c-ip'};
        my $date        = $row->{'date'};
        my $time        = $row->{'time'};
        my $duration    = $row->{'x-duration'};
        my $status      = $row->{'c-status'};
        my $referrer    = $row->{'cs(Referer)'};
        push @parsed, [ $ip, $date, $time, $stream_name, $duration, $status, $referrer ];
    }
    return @parsed;
}

# helper function for _parse_log_file
sub _get_column_names {
    my $fh = shift;
    my $csv = Text::CSV_XS->new( { q/binary/ => 0, q/sep_char/ => $CONFIG{'input_delimiter'} } );

    my @column_names;
    while ( my $line = <$fh> ) {
        if ( $line =~ /^[#]Fields/xsm ) {    # go through the file line by line until we hit a line starting with "#Fields"
            $csv->parse($line) or croak( q/Failed to parse field: / . Text::CSV_XS->error_diag() );
            @column_names = $csv->fields;

            # the first element of @column_names will be '#Fields:'.  Throw it away.
            shift @column_names;
            return @column_names;
        }

    }
    croak(q/Failed to find column headings/);
}

# Get a list of WMS logs for the last quarter in the given directory
sub _get_last_quarter_wmslogs {
    my $dir    = shift;
    my $number = shift;
    my ( $quarter_begin, $quarter_end ) = _dates_for_last_quarter();

    my @interval_files;

    my $days;
    if ( !-e $dir ) {
        croak(qq/directory "$dir" could not be found/);
    }
    if ( !-d $dir ) {
        croak(qq/"$dir" is not a directory/);
    }
    my @files = reverse _list_dir($dir);    # we expect files to be named by date

    my $start_date = $quarter_end;
    for my $file (@files) {
        chomp $file;
        ( my $file_date = $file ) =~ s/\D+(\d+).+/$1/xsm;
        if ( $file_date > $quarter_end ) {    # disregard anything after the end of the last quarter
            next;
        }
        if ( $file_date < $quarter_begin ) {    # We don't need anything before the beginning of the last quarter
            last;
        }
        my $interval_ok = _is_interval_ok( $start_date, $file_date );

        if ($interval_ok) {
            $days ||= 1;                        # Account for the first log file found
            if ( $start_date != $file_date ) {  # don't increment $days for multiple files on the same date
                $days++;
            }
            if ( $days <= $number ) {
                push @interval_files, $file;
            }
        }
        else {                                  # there's a gap.
            @interval_files = ();
            $days           = 0;
        }
        $start_date = $file_date;
        if ( $days > $number ) {                # This is a little inelegant in light of the if statement above, but it works
            last;
        }
    }
    if ( $days < $number ) {
        croak("Failed to find $number consecutive days.");
    }
    return @interval_files;
}

# get the most recently completed calendar quarter
sub _get_last_quarter {
    my $now = localtime;

    my $q1_begin = Time::Piece->strptime( qq/January 01 $YEAR/, q/%B %d %Y/ );
    my $q2_begin = Time::Piece->strptime( qq/April 01 $YEAR/,   q/%B %d %Y/ );
    my $q3_begin = Time::Piece->strptime( qq/July 01 $YEAR/,    q/%B %d %Y/ );
    my $q4_begin = Time::Piece->strptime( qq/October 01 $YEAR/, q/%B %d %Y/ );
    if ( $now >= $q4_begin ) {    # Q4 begins 1 October
        return q/Q3/;
    }
    elsif ( $now >= $q3_begin ) {    # Q3 begins 1 July
        return q/Q2/;
    }
    elsif ( $now >= $q2_begin ) {    # Q2 begins 1 April
        return q/Q1/;
    }
    else {                           # Q1 begins 1 January
        return q/Q4/;
    }
}

# returns starting and ending days (in that order) of the last quarter in YYMMDD format
sub _dates_for_last_quarter {
    my $now = localtime;

    my $last_quarter = _get_last_quarter();
    if ( $last_quarter eq 'Q3' ) {
        return ( qq/${YEAR}0701/, qq/${YEAR}0930/ );
    }
    elsif ( $last_quarter eq 'Q2' ) {
        return ( qq/${YEAR}0401/, qq/${YEAR}0630/ );
    }
    elsif ( $last_quarter eq 'Q1' ) {
        return ( qq/${YEAR}0101/, qq/${YEAR}0331/ );
    }
    else {
        my $last_year = $YEAR - 1;
        return ( qq/${last_year}1001/, qq/${last_year}1231/ );
    }
}

sub _is_interval_ok {
    my $start_date = shift;
    my $end_date   = shift;

    my $start = Time::Piece->strptime( $start_date, q/%Y%m%d/ );
    my $end   = Time::Piece->strptime( $end_date,   q/%Y%m%d/ );
    my $interval = $start_date - $end_date;
    if ( $interval > 1 ) {
        return 0;
    }
    return 1;
}

sub email_logs {
    my $logs_dir = shift;
    
    my $subject = qq/$CONFIG{'call_letters'} $LAST_QUARTER $YEAR SoundExchange logs/;
    my $message_text = qq/See attached zip file/;

    my $log_files = [ map { qq{$logs_dir/$_}; } _list_dir($logs_dir) ];
    my $compressed_logs = compress_logs($log_files, 1);

    my $attachment_filename = qq/$CONFIG{'call_letters'} $LAST_QUARTER $YEAR SoundExchange Logs.zip/;
    my $logs_attachment = Email::MIME->create(
        'attributes' => {
            'filename' => $attachment_filename,
            'content_type' => q{application/zip},
            'encoding' => q/base64/,
        },
        'body' => io($compressed_logs)->all,
    );
    my $text_attachment = Email::MIME->create(
        'attributes' => {
            'content_type' => q{text/plain},
            'disposition' => q/inline/,
            'charset' => q/US-ASCII/,
            'encoding' => q/quoted-printable/,
        },
        'body_str' => $message_text,
    );
    my $parts = [
        $text_attachment,
        $logs_attachment,
    ];

    unlink($compressed_logs);
    my $email = Email::MIME->create(
        'header_str' => [ 
            'From' => $CONFIG{'notify_from'}, 
            'To' => $CONFIG{'notify_to'},
            'Subject' => $subject,
        ],
        'parts' => $parts,
    );

    eval {
        _mail( 
            { 
                'from' => $CONFIG{'notify_from'}, 
                'to' => $CONFIG{'notify_to'}, 
                'body' => $email->as_string,
            }
        );
        1;
    } or croak(qq/Failed to send compressed logs: $EVAL_ERROR/);

    return 1;
}

sub _mail {
    my $args = shift;
    my $mailer = Net::SMTP->new( $CONFIG{'smtp_host'}, Hello => hostname, );
    if ( !$mailer ) {
        croak qq/Failed to connect to $CONFIG{'smtp_host'}: $OS_ERROR/;
    }

    $mailer->mail( $args->{'from'} );
    $mailer->to( $args->{'to'} );
    $mailer->data;
    $mailer->datasend( $args->{'body'} );
    $mailer->dataend;
    $mailer->quit;
    return 1;
}

sub compress_logs {
    my ($files_to_compress, $delete_after_add) = @_;
    
    my $zip = Archive::Zip->new();
    my $usage = q/Usage: compress_logs(<files_to_compress>, <output_file>)/;
    
    if (ref $files_to_compress ne q/ARRAY/) {
        croak(qq/$usage\nFirst argument must be an arrayref/);
    }

    for my $file (@{$files_to_compress}) {
        my $filename = [File::Spec->splitpath($file)]->[2];
        eval {
            $zip->addFile($file, $filename);
            1;
        } or croak("Failed to add file $file: $EVAL_ERROR");
    }
    
    my ($fh, $output_file) = File::Temp::tempfile();
    eval {
        my $status = $zip->writeToFileHandle($fh);
        1;
    } or do {
        croak(qq/Failed to write zip file: $EVAL_ERROR/);
    };
    close $fh;
        
    if ($delete_after_add) {
        for my $file (@{$files_to_compress}) {
            unlink $file;
        }
    }

    return $output_file;
}

sub _list_dir {
    my $dir = shift;

    my @files;
    opendir my $dh, $dir or die "Can't open $dir: $!";
    foreach my $entry (readdir $dh) {
        if ($entry ne q{.} && $entry ne q{..}) {
            push @files, $entry;
        }
    } 
    closedir $dh;
    return @files;
}

# pull data from a CSV export of the compilations table
sub _get_compilations_data {
    open my $fh, '<', $CONFIG{'compilations_data_file'};
    my $csv = Text::CSV_XS->new(
        {
            'sep_char'           => q{,},
            'binary'             => 1,
            'allow_loose_quotes' => 1
        }
    );
    my $header = $csv->getline($fh);
    my $compilations_data = {};
    
    my $row = {};
    $csv->bind_columns( \@{$row}{@{$header}} );
    while ($csv->getline($fh)) {
        @{ $compilations_data->{ $row->{'Song_ID'} } }{keys %{$row}} = values %{$row};
    }
    return $compilations_data;
}

1;
