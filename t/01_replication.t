use strict;
use warnings;

use DBI;
use Test::More;
use Test::Exception;
use Test::mysqld;
use Test::TCP;
use Guard;

sub merge_cap_cmd {
    my ($cmdref) = @_;
    pipe my $logrh, my $logwh
        or die "Died: failed to create pipe:$!\n";
    my $pid = fork;
    if ( ! defined $pid ) {
        die "Died: fork failed: $!\n";
    }

    elsif ( $pid == 0 ) {
        #child
        close $logrh;
        open STDOUT, '>&', $logwh
            or die "Died: failed to redirect STDOUT\n";
        open STDERR, '>&', $logwh
            or die "Died: failed to redirect STDERR\n";
        close $logwh;
        exec @$cmdref;
        die "Died: exec failed: $!\n";
    }
    close $logwh;
    my $result;
    while(<$logrh>){
        $result .= $_;
    }
    close $logrh;
    while (wait == -1) {}
    my $exit_code = $?;
    $exit_code = $exit_code >> 8;
    return ($result, $exit_code);
}


# setup replication
# copy from http://d.hatena.ne.jp/ZIGOROu/20100330/1269971484

sub setup_master {
    my $mysqld = Test::mysqld->new(
        my_cnf     => +{
            'port'      => empty_port(),
            'log-bin'   => 'mysql-bin',
            'server-id' => 1,
        },
    ) or die($Test::mysqld::errstr);
    note( $mysqld->dsn );
    my $dbh = DBI->connect( $mysqld->dsn, 'root', '' );
    $dbh->do(
        sprintf(
            q|CREATE USER '%s'@'%s' IDENTIFIED BY '%s'|,
            'repl', '127.0.0.1', 'replpass'
        )
    ) or die( $dbh->errstr );
    $dbh->do(
        sprintf(
            q|GRANT REPLICATION SLAVE ON *.* TO '%s'@'%s'|,
            'repl', '127.0.0.1'
        )
    ) or die( $dbh->errstr );
    return $mysqld;
}

sub setup_slave {
    my $master_mysqld = shift;
    my $mysqld = Test::mysqld->new(
        my_cnf     => +{
            'port'      => empty_port(),
            'server-id' => 2,
        },
    ) or die($Test::mysqld::errstr);
    note( $mysqld->dsn );
    my $dbh_master = DBI->connect( $master_mysqld->dsn, 'root', '' );
    my $master_status = $dbh_master->selectrow_hashref( 'SHOW MASTER STATUS' );
    my $dbh = DBI->connect( $mysqld->dsn, 'root', '' );
    $dbh->do(
        sprintf(
            q|CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_LOG_FILE='%s', MASTER_LOG_POS=%d|,
            '127.0.0.1', $master_mysqld->my_cnf->{port},
            'repl', 'replpass', $master_status->{File}, $master_status->{Position},
        )
    );
    $dbh->do(q|START SLAVE|);
    sleep 2;
    my $rows;
    note(
        explain(
            $rows = $dbh->selectall_arrayref( 'SHOW SLAVE STATUS', +{ Slice => +{} } )
        )
    );
    is($rows->[0]->{Slave_SQL_Running},"Yes");
    is($rows->[0]->{Slave_IO_Running},"Yes");

    $dbh_master->do(
        q|CREATE TABLE boofy ( id INT NOT NULL PRIMARY KEY, name VARCHAR(32), sig INT NOT NULL ) ENGINE=InnoDB|
    ) or die($dbh_master->errstr);
    $dbh_master->do(
        q|CREATE TABLE moofy ( id INT NOT NULL PRIMARY KEY, sig INT NOT NULL ) ENGINE=InnoDB|
    ) or die($dbh_master->errstr);
    for my $k (1..3) {
        $dbh_master->do( q|INSERT INTO boofy(id,name,sig) VALUES(?,?,?)|, undef, $k,'bk', 1 ) or die($dbh_master->errstr);
        $dbh_master->do( q|INSERT INTO moofy(id,sig) VALUES(?,?)|, undef, $k, 1 ) or die($dbh_master->errstr);
    }
    sleep 1;
    $dbh->do(
        sprintf(
            q|SET PASSWORD FOR 'root'@'localhost'=PASSWORD('%s')|,
            '2qX84GUPMcfD02L'
        )
    ) or die( $dbh->errstr );

    $rows = $dbh->selectall_arrayref( 'SELECT * FROM boofy', +{ Slice => +{} } );
    is($rows->[0]->{id}, 1);
    is($rows->[0]->{name}, 'bk');

    return $mysqld;
}

sub master_updater {
    my $master_dsn = shift;
    my @pids;
    for my $k (1..3) {
        my $pid = fork;
        die $! unless defined $pid;
        if ( $pid == 0 ) {
            # child
            my $stop = 0;
            local $SIG{TERM} = sub {
                $stop = 1;
            };
            my $dbh = DBI->connect( $master_dsn, 'root', '' );
            foreach  (1..10000) {
                last if $stop;
                $dbh->begin_work or die $dbh->errstr;
                $dbh->do(q!UPDATE moofy SET sig=sig+1 WHERE id=?!,undef, $k) or die $dbh->errstr;
                select undef, undef, undef, 0.001;
                my $rows = $dbh->selectall_arrayref( "SELECT * FROM boofy WHERE id=$k FOR UPDATE", +{ Slice => +{} } )
                    or die $dbh->errstr;
                my $sig = $rows->[0]->{sig} + 1;
                $dbh->do(q!UPDATE boofy SET sig=? WHERE id=?!,undef,$sig, $k)
                    or die $dbh->errstr;
                $dbh->commit
                    or die $dbh->errstr;
            }
            exit;
        }
        #parent
        push @pids, $pid;
    }
    # parent
    my $guard = guard {
        for (@pids) {
            kill('TERM',$_);
            waitpid $_, 0;
        }
    };
    return $guard;
}

my $master_mysqld;
lives_ok(
    sub {
        $master_mysqld = setup_master;
    },
    'setup_master() is success'
);

my $slave_mysqld;
lives_ok(
    sub {
        $slave_mysqld = setup_slave($master_mysqld);
    },
    'setup_slave() is success'
);

{
    my ($resut, $exit_code) = merge_cap_cmd(['./safe-slave-stop','-H','localhost','-p',$slave_mysqld->my_cnf->{port},'-u','root','-P','2qX84GUPMcfD02L']);
    is($exit_code,0,"success");
    note($resut);
    like($resut, qr/Slave just stopped safety/);
}

{
    my $dbh = DBI->connect( $slave_mysqld->dsn, 'root', '2qX84GUPMcfD02L' );
    my $rows = $dbh->selectall_arrayref( 'SHOW SLAVE STATUS', +{ Slice => +{} } );
    is($rows->[0]->{Slave_SQL_Running},"No");
    is($rows->[0]->{Slave_IO_Running},"No");
    $dbh->do(q|START SLAVE|);
    sleep 2;
    $rows = $dbh->selectall_arrayref( 'SHOW SLAVE STATUS', +{ Slice => +{} } );
    is($rows->[0]->{Slave_SQL_Running},"Yes");
    is($rows->[0]->{Slave_IO_Running},"Yes");
}

my $master_updater = master_updater($master_mysqld->dsn);

sleep 1;

for (1..5) {
    my ($resut, $exit_code) = merge_cap_cmd(['./safe-slave-stop','-H','localhost','-p',$slave_mysqld->my_cnf->{port},'-u','root','-P','2qX84GUPMcfD02L']);
    is($exit_code,0,"success");
    note($resut);
    like($resut, qr/Slave just stopped safety/);
    my $dbh = DBI->connect( $slave_mysqld->dsn, 'root', '2qX84GUPMcfD02L' );
    my $rows = $dbh->selectall_arrayref( 'SELECT * FROM boofy', +{ Slice => +{} } );
    my $rows2 = $dbh->selectall_arrayref( 'SELECT * FROM moofy', +{ Slice => +{} } );
    ok($rows->[0]->{sig} > 1);
    is($rows->[0]->{sig}, $rows2->[0]->{sig});
    $dbh->do(q|START SLAVE|);
    sleep 1;
}

done_testing();

undef $master_updater;
undef $master_mysqld;
