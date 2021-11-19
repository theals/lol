#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use Data::Dumper;
use JSON;
use LWP::UserAgent;
use Scalar::Util qw(looks_like_number);

my $apiKey = "RGAPI-77af31f0-adeb-4eb0-951c-6d5df0664805";
my $arqSummoners = "/home/pi/perl5/summoners.txt";

my $apiBase = "https://br1.api.riotgames.com";
my $apiBaseAmericas = "https://americas.api.riotgames.com";
my $apiSummoners = $apiBase."/lol/summoner/v4/summoners";
my $apiMatches = $apiBaseAmericas."/lol/match/v5/matches";
my $fieldLastUpdate = "last_insert_update";
my $fieldJSON = "jsonField";
my @summoners = ();

my $lolDB = "/home/pi/perl5/leagueOfLegends.db";
my $maxRetry = 3;
my $numRetry = $maxRetry;

my $fh;
open($fh,$arqSummoners) or die $!;
while (my $linha=<$fh>) {
	$linha =~ s/[\r\n]+//g;
	push(@summoners,$linha);
}
close($fh);

#print Dumper(\@summoners);

my $db = DBI->connect("dbi:SQLite:dbname=".$lolDB,"","") or die DBI->errstr;

###
### Passando pelos summoners
###

# UserAgent padrao
my $ua = LWP::UserAgent->new();
$ua->default_headers->header( "X-Riot-Token" => $apiKey );
$ua->timeout( 10 );

SUMMONERS:
foreach my $nome (@summoners) {
	###
	### Passo 1 - Obtendo os dados do summoner
	###

	print "Summoner: ".$nome."\n";

	# Verificando se ja existe na base de dados
	my $jsonSummoner = undef;
	{
		my $sth = $db->prepare('SELECT '.$fieldJSON.' FROM summoners WHERE name='.$db->quote($nome));
		if (defined($sth)) {
			$sth->execute() or die $db->errstr;
			($jsonSummoner) = $sth->fetchrow_array();
		}
	}

	my ($refSumm);
	if (!defined($jsonSummoner)) {
		sleep(2);
		my $url = $apiSummoners."/by-name/".$nome;
		my $resp = $ua->get($url);

		if (!$resp->is_success) {
		
			print "ERRO - nao consegui obter informacoes sobre ".$nome." (".$resp->code." - ".$resp->message.")\n";
			print "URL: ".$url."\n";
			exit;
		}

		$jsonSummoner = $resp->decoded_content;
		$refSumm = decode_json($jsonSummoner);

		&insertContent($db,"summoners",$refSumm,'puuid',$jsonSummoner,undef);
	} else {
		$refSumm = decode_json($jsonSummoner);
	}

	###
	### Passo 2 - Obtendo todos os jogos jogados
	###

	my ($jsonListMatches,$refListMatches);
	my @listIds = ();
	{
		my $start = 0;
		my $count = 100;
		my $returned = 100;
		my $numJogo = 0;

		while ($returned == $count) {
			my $retry = 1;
			$numRetry = $maxRetry;
			print "\tObtendo ".$count." jogos (iniciando em ".$start.")...\n";
			my ($url,$resp);
			while ($retry == 1 && $numRetry > 0) {
				--$numRetry;
				sleep(2);
				$url = $apiMatches."/by-puuid/".$refSumm->{'puuid'}."/ids?start=".$start."&count=".$count;
				$resp = $ua->get($url);

				if (!$resp->is_success) {
					print "ERRO - nao consegui obter informacoes de jogos sobre ".$nome." (".$resp->code.": ".$resp->message.")\n";
					$retry = 1;
				} else {
					$retry = 0;
				}
			}

			if ($numRetry <= 0) { print "Falhou - trabalhando proximo summoner...\n"; next SUMMONERS; }
			
			$jsonListMatches = $resp->decoded_content;
			$refListMatches = decode_json($jsonListMatches);

			my %ret = (
					puuid => $refSumm->{'puuid'},
					matchId => 0,
					gameCreation => 0
				);
			&createContent($db,"listMatchesPuuid",\%ret);

			#print Dumper($refListMatches);

			$returned = scalar(@$refListMatches);
			print "\t".$returned." retornados\n";

			my $sth2 = $db->prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?") or die $db->errstr;
			$sth2->execute("matches") or die $db->errstr;

			my ($tabelaExiste) = $sth2->fetchrow_array();

			my $sth;
			my %listaIDGames = ();
			if (defined($tabelaExiste)) {
				#$sth = $db->prepare("SELECT matchId FROM matches WHERE matchId = ?") or die $db->errstr;
				my $sql = "SELECT matchId FROM matches WHERE matchId in ('".join("','",@$refListMatches)."')";
				#print "SQL: $sql\n";
				$sth = $db->prepare($sql) or die $db->errstr;
				$sth->execute or die $db->errstr;
				
				my $listGames = $sth->fetchall_arrayref;
				
				foreach my $r (@$listGames) {
					$listaIDGames{$r->[0]} = 1;
				}
			}

			JOGO:
			foreach my $matchId (@$refListMatches) {
				# Obtendo informacoes do jogo especifico
				++$numJogo;

				if (exists($listaIDGames{$matchId})) { next JOGO; }

				print " \t\tLendo jogo ".$numJogo."...\n";
				my $retry = 1;
				$numRetry = $maxRetry;
				while ($retry == 1 && $numRetry > 0) {
					--$numRetry;
					sleep(2);
					$url = $apiMatches."/".$matchId;
					$resp = $ua->get($url);

					if (!$resp->is_success) {
						print "ERRO - nao consegui obter informacoes do jogo ".$matchId." - tentando de novo\n";
						$retry = 1;
					} else {
						$retry = 0;
					}
				}

				if ($numRetry <= 0) { print "Falhou - pulando jogo...\n"; next JOGO; }
				
				my $jsonMatch = $resp->decoded_content;
				my $refMatch = decode_json($jsonMatch);

				#print Dumper($refMatch);

				# Informacoes para referenciar cada jogador ao jogo
				foreach my $puuid (@{$refMatch->{'metadata'}->{'participants'}}) {
					my %dataListMatch = (
							puuid => $puuid,
							matchId => $matchId,
							gameCreation => $refMatch->{'info'}->{'gameCreation'}
						);
					#print Dumper(\%dataListMatch);
					&insertContent($db,"listMatchesPuuid",\%dataListMatch,undef,undef,undef);
				}

				# Armazenando informacoes do jogo em si
				my %camposInsert = (
						matchId => $refMatch->{'metadata'}->{'matchId'},
						gameCreation => $refMatch->{'info'}->{'gameCreation'},
						gameDuration => $refMatch->{'info'}->{'gameDuration'},
						gameStartTimestamp => $refMatch->{'info'}->{'gameStartTimestamp'}
					);
#	my ($db,$table,$ref,$campoChave,$jsonText,$camposInsert) = @_;

				&insertContent($db,"matches",$refMatch,"matchId",$jsonMatch,\%camposInsert);
			}

			$start += $count;
		}
	}
}

$db->disconnect;

###
### FUNCOES
###

sub createContent {
	my ($db,$table,$ref,$camposInsert) = @_;
	{
		my $sth = $db->prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?") or die $db->errstr;
		$sth->execute($table) or die $db->errstr;

		my ($n) = $sth->fetchrow_array();

		# Criando tabela se nao existir
		if (!defined($n)) {
			my @campos = ();
			foreach my $key (keys(%{$ref})) {
				if (ref $ref->{$key} eq "") {
					my @campo = ($key);
					if (looks_like_number($ref->{$key})) {
						push(@campo,"NUMBER");
					} else {
						push(@campo,"VARCHAR2");
					}

					push(@campos,$campo[0]." ".$campo[1]);
				}
			}
			push(@campos,$fieldLastUpdate." DATE");
			push(@campos,$fieldJSON." TEXT");

			# Criando campos solicitados
			while (my ($k,$v) = each(%$camposInsert)) {
				push(@campos,$k." TEXT");
			}

			my $sql = "CREATE TABLE ".$table." (".join(",",@campos).")";
			$db->do($sql) or die $db->errstr;
		}
	}

}

sub insertContent {
	my ($db,$table,$ref,$campoChave,$jsonText,$camposInsert) = @_;

	&createContent($db,$table,$ref,$camposInsert);

	# Integrando campos para Insert
	while (my ($k,$v) = each(%$camposInsert)) {
		$ref->{$k} = $v;
	}

	# Verificando se o registro ja existe
	if(defined($campoChave)) {
		my $sqlCheck = "SELECT ".$campoChave." FROM ".$table." WHERE ".$campoChave."=".$db->quote($ref->{$campoChave});
		my $sth = $db->prepare($sqlCheck) or die $db->errstr;
		$sth->execute() or die $db->errstr;
		my ($valor) = $sth->fetchrow_array();

		if (defined($valor)) {
			# Registro ja inserido - retornando
			return 1;
		}
	}

	# Inserindo o dado
	my @campos = ();
	my @valores = ();
	foreach my $key (keys(%$ref)) {
			if (ref $ref->{$key} eq "") {
				push(@campos,$key);
				push(@valores,$ref->{$key});
			}
	}

	my $sql = 'INSERT INTO '.$table.' ('.join(",",@campos).','.$fieldLastUpdate.','.$fieldJSON.') VALUES ("'.join('","',@valores).'",date(\'now\'),'.$db->quote($jsonText).')';
	$db->begin_work() or die $db->errstr;
	if (!$db->do($sql)) {
		print "SQL: $sql\n";
		print Dumper($ref);
		die $db->errstr;
	}
	$db->commit() or die $db->errstr;
}
