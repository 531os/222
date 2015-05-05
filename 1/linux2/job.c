#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <string.h>
#include <signal.h>
#include <fcntl.h>
#include <time.h>
#include "job.h"

//#define DEBUG

int jobid=0;
int siginfo=1;
int fifo;
int globalfd;


readyqueue *Q=NULL,*head=NULL;

readyqueue *next=NULL,*current =NULL;

void creat_Q()
{
	readyqueue *tmp,*head1=NULL;
	int i=0;
	current=Q=head1;
	for(;i<3;i++)
	{
		if((tmp = (readyqueue *)malloc(sizeof(readyqueue)))==NULL)  
		{
		   perror("malloc");  
		   exit(1);  
		}
		tmp->wait_Q=NULL;
		tmp ->prio = i+1;  
		if(i+1==1)
			tmp->round=1;
		if(i+1==2)
			tmp->round=2;
		if(i+1==3)
			tmp->round=5;  
		if(head1==NULL)
			{head1=tmp;continue;}
		else
			head1 ->next = tmp;
		head1=head1->next;
		head1->next=NULL;
	}
}
/* µ÷¶È³ÌÐò */
void scheduler()
{
	struct jobinfo *newjob=NULL;
	struct jobcmd cmd;
	int  count = 0;
	bzero(&cmd,DATALEN);
	if((count=read(fifo,&cmd,DATALEN))<0)
		error_sys("read fifo failed");
#ifdef DEBUG

	if(count){
		printf("cmd cmdtype\t%d\ncmd defpri\t%d\ncmd data\t%s\n",cmd.type,cmd.defpri,cmd.data);
	}
	else
		printf("no data read\n");
#endif

	/* žüÐÂµÈŽý¶ÓÁÐÖÐµÄ×÷Òµ */
	updateall();

	switch(cmd.type){
	case ENQ:
		do_enq(newjob,cmd);
		break;
	case DEQ:
		do_deq(cmd);
		break;
	case STAT:
		do_stat(cmd);
		break;
	default:
		break;
	}

	/* Ñ¡ÔñžßÓÅÏÈŒ¶×÷Òµ */
	next=jobselect();
	/* ×÷ÒµÇÐ»» */
	jobswitch();
}

int allocjid()
{
	return ++jobid;
}

void updateall()
{
	struct queue *p;

	/* žüÐÂ×÷ÒµÔËÐÐÊ±Œä */
	if(current)
		current->job->run_time += 1; /* ŒÓ1Žú±í1000ms */

	/* žüÐÂ×÷ÒµµÈŽýÊ±ŒäŒ°ÓÅÏÈŒ¶ */
	for(p = head; p != NULL; p = p->next){
		p->job->wait_time += 1000;
		if(p->job->wait_time >= 5000 && p->job->curpri < 3){
			p->job->curpri++;
			p->job->wait_time = 0;
		}
	}
}

struct queue* jobselect()
{
	struct queue *p,*prev,*select,*selectprev;
	int highest = -1;

	select = NULL;
	selectprev = NULL;
	if(head){
		/* ±éÀúµÈŽý¶ÓÁÐÖÐµÄ×÷Òµ£¬ÕÒµœÓÅÏÈŒ¶×îžßµÄ×÷Òµ */
		for(prev = head, p = head; p != NULL; prev = p,p = p->next)
			if(p->job->curpri > highest){
				select = p;
				selectprev = prev;
				highest = p->job->curpri;
			}
			selectprev->next = select->next;
			if (select == selectprev)
				head = NULL;
	}
	return select;
}

void jobswitch()
{
	struct queue *p;
	int i;
	#ifdef DEBUG
		printf("job is switching!\n");
		printf("%d",current->job->pid);
	#endif


	if(current && current->job->state == DONE){ /* µ±Ç°×÷ÒµÍê³É */
		/* ×÷ÒµÍê³É£¬ÉŸ³ýËü */
		for(i = 0;(current->job->cmdarg)[i] != NULL; i++){
			free((current->job->cmdarg)[i]);
			(current->job->cmdarg)[i] = NULL;
		}
		/* ÊÍ·Å¿ÕŒä */
		free(current->job->cmdarg);
		free(current->job);
		free(current);

		current = NULL;
	}

	if(next == NULL && current == NULL) /* Ã»ÓÐ×÷ÒµÒªÔËÐÐ */

		return;
	else if (next != NULL && current == NULL){ /* ¿ªÊŒÐÂµÄ×÷Òµ */

		printf("begin start new job\n");
		current = next;
		next = NULL;
		current->job->state = RUNNING;
		kill(current->job->pid,SIGCONT);
		return;
	}
	else if (next != NULL && current != NULL){ /* ÇÐ»»×÷Òµ */

		printf("switch to Pid: %d\n",next->job->pid);
		kill(current->job->pid,SIGSTOP);
		current->job->curpri = current->job->defpri;
		current->job->wait_time = 0;
		current->job->state = READY;

		/* ·Å»ØµÈŽý¶ÓÁÐ */
		if(head){
			for(p = head; p->next != NULL; p = p->next);
			p->next = current;
		}else{
			head = current;
		}
		current = next;
		next = NULL;
		current->job->state = RUNNING;
		current->job->wait_time = 0;
		kill(current->job->pid,SIGCONT);
		return;
	}else{ /* next == NULLÇÒcurrent != NULL£¬²»ÇÐ»» */
		return;
	}
}

void sig_handler(int sig,siginfo_t *info,void *notused)
{
	int status;
	int ret;

	switch (sig) {
case SIGVTALRM: /* µœŽïŒÆÊ±Æ÷ËùÉèÖÃµÄŒÆÊ±Œäžô */
	scheduler();
	#ifdef DEBUG
		printf("SIGVTALRM RECEVIRD!");
	#endif
	return;
case SIGCHLD: /* ×Óœø³ÌœáÊøÊ±Ž«ËÍžøžžœø³ÌµÄÐÅºÅ */
	ret = waitpid(-1,&status,WNOHANG);
	if (ret == 0)
		return;
	if(WIFEXITED(status)){
		current->job->state = DONE;
		printf("normal termation, exit status = %d\n",WEXITSTATUS(status));
	}else if (WIFSIGNALED(status)){
		printf("abnormal termation, signal number = %d\n",WTERMSIG(status));
	}else if (WIFSTOPPED(status)){
		printf("child stopped, signal number = %d\n",WSTOPSIG(status));
	}
	return;
	default:
		return;
	}
}

void do_enq(struct jobinfo *newjob,struct jobcmd enqcmd)
{
	struct queue *newnode,*p;
	int i=0,pid;
	char *offset,*argvec,*q;
	char **arglist;
	sigset_t zeromask;

	sigemptyset(&zeromask);

	/* ·â×°jobinfoÊýŸÝœá¹¹ */
	newjob = (struct jobinfo *)malloc(sizeof(struct jobinfo));
	newjob->jid = allocjid();
	newjob->defpri = enqcmd.defpri;
	newjob->curpri = enqcmd.defpri;
	newjob->ownerid = enqcmd.owner;
	newjob->state = READY;
	newjob->create_time = time(NULL);
	newjob->wait_time = 0;
	newjob->run_time = 0;
	arglist = (char**)malloc(sizeof(char*)*(enqcmd.argnum+1));
	newjob->cmdarg = arglist;
	offset = enqcmd.data;
	argvec = enqcmd.data;
	while (i < enqcmd.argnum){
		if(*offset == ':'){
			*offset++ = '\0';
			q = (char*)malloc(offset - argvec);
			strcpy(q,argvec);
			arglist[i++] = q;
			argvec = offset;
		}else
			offset++;
	}

	arglist[i] = NULL;

#ifdef DEBUG

	printf("enqcmd argnum %d\n",enqcmd.argnum);
	for(i = 0;i < enqcmd.argnum; i++)
		printf("parse enqcmd:%s\n",arglist[i]);

#endif

	/*ÏòµÈŽý¶ÓÁÐÖÐÔöŒÓÐÂµÄ×÷Òµ*/
	newnode = (struct queue*)malloc(sizeof(struct queue));
	newnode->next =NULL;
	newnode->job=newjob;

	if(head)
	{
		for(p=head;p->next != NULL; p=p->next);
		p->next =newnode;
	}else
		head=newnode;

	/*Îª×÷ÒµŽŽœšœø³Ì*/
	if((pid=fork())<0)
		error_sys("enq fork failed");

	if(pid==0){
		newjob->pid =getpid();
		/*×èÈû×Óœø³Ì,µÈµÈÖŽÐÐ*/
		raise(SIGSTOP);
#ifdef DEBUG

		printf("begin running\n");
		for(i=0;arglist[i]!=NULL;i++)
			printf("arglist %s\n",arglist[i]);
#endif

		/*žŽÖÆÎÄŒþÃèÊö·ûµœ±ê×ŒÊä³ö*/
		dup2(globalfd,1);
		/* ÖŽÐÐÃüÁî */
		if(execv(arglist[0],arglist)<0)
			printf("exec failed\n");
		exit(1);
	}else{
		newjob->pid=pid;
	}
}

void do_deq(struct jobcmd deqcmd)
{
	int deqid,i;
	struct queue *p,*prev,*select,*selectprev;
	deqid=atoi(deqcmd.data);

#ifdef DEBUG
	printf("deq jid %d\n",deqid);
#endif

	/*current jodid==deqid,ÖÕÖ¹µ±Ç°×÷Òµ*/
	if (current && current->job->jid ==deqid){
		printf("teminate current job\n");
		kill(current->job->pid,SIGKILL);
		for(i=0;(current->job->cmdarg)[i]!=NULL;i++){
			free((current->job->cmdarg)[i]);
			(current->job->cmdarg)[i]=NULL;
		}
		free(current->job->cmdarg);
		free(current->job);
		free(current);
		current=NULL;
	}
	else{ /* »òÕßÔÚµÈŽý¶ÓÁÐÖÐ²éÕÒdeqid */
		select=NULL;
		selectprev=NULL;
		if(head){
			for(prev=head,p=head;p!=NULL;prev=p,p=p->next)
				if(p->job->jid==deqid){
					select=p;
					selectprev=prev;
					break;
				}
				selectprev->next=select->next;
				if(select==selectprev)
					head=NULL;
		}
		if(select){
			for(i=0;(select->job->cmdarg)[i]!=NULL;i++){
				free((select->job->cmdarg)[i]);
				(select->job->cmdarg)[i]=NULL;
			}
			free(select->job->cmdarg);
			free(select->job);
			free(select);
			select=NULL;
		}
	}
}

void do_stat(struct jobcmd statcmd)
{
	struct queue *p;
	char timebuf[BUFLEN];
	/*
	*ŽòÓ¡ËùÓÐ×÷ÒµµÄÍ³ŒÆÐÅÏ¢:
	*1.×÷ÒµID
	*2.œø³ÌID
	*3.×÷ÒµËùÓÐÕß
	*4.×÷ÒµÔËÐÐÊ±Œä
	*5.×÷ÒµµÈŽýÊ±Œä
	*6.×÷ÒµŽŽœšÊ±Œä
	*7.×÷Òµ×ŽÌ¬
	*/

	/* ŽòÓ¡ÐÅÏ¢Í·²¿ */
	printf("JOBID\tPID\tOWNER\tRUNTIME\tWAITTIME\tCREATTIME\t\tSTATE\n");
	if(current){
		strcpy(timebuf,ctime(&(current->job->create_time)));
		timebuf[strlen(timebuf)-1]='\0';
		printf("%d\t%d\t%d\t%d\t%d\t%s\t%s\n",
			current->job->jid,
			current->job->pid,
			current->job->ownerid,
			current->job->run_time,
			current->job->wait_time,
			timebuf,"RUNNING");
	}

	for(p=head;p!=NULL;p=p->next){
		strcpy(timebuf,ctime(&(p->job->create_time)));
		timebuf[strlen(timebuf)-1]='\0';
		printf("%d\t%d\t%d\t%d\t%d\t%s\t%s\n",
			p->job->jid,
			p->job->pid,
			p->job->ownerid,
			p->job->run_time,
			p->job->wait_time,
			timebuf,
			"READY");
	}
}

int main()
{
	struct timeval interval;
	struct itimerval new,old;
	struct stat statbuf;
	struct sigaction newact,oldact1,oldact2;
	printf("error");
	//creat_Q();
	//head=Q;
	#ifdef DEBUG
		printf("Debug is open!");
	#endif
	/*
	struct sigaction{
  	void (*sa_handler)(int);
   	sigset_t sa_mask;
  	int sa_flag;
  	void (*sa_sigaction)(int,siginfo_t *,void *);
	};*/
		

	if(stat("/tmp/server",&statbuf)==0){
		/* Èç¹ûFIFOÎÄŒþŽæÔÚ,ÉŸµô */
		if(remove("/tmp/server")<0)
			error_sys("remove failed");
	}

	if(mkfifo("/tmp/server",0666)<0)
		error_sys("mkfifo failed");
	/* ÔÚ·Ç×èÈûÄ£ÊœÏÂŽò¿ªFIFO */
	if((fifo=open("/tmp/server",O_RDONLY|O_NONBLOCK))<0)
		error_sys("open fifo failed");

	/* œšÁ¢ÐÅºÅŽŠÀíº¯Êý */
	newact.sa_sigaction=sig_handler;
	sigemptyset(&newact.sa_mask);
	newact.sa_flags=SA_SIGINFO;
	sigaction(SIGCHLD,&newact,&oldact1);
	sigaction(SIGVTALRM,&newact,&oldact2);

	/* ÉèÖÃÊ±ŒäŒäžôÎª1000ºÁÃë */
	if(current==NULL)
		printf("NULL error ");
	else printf("error");
	interval.tv_sec=current->round;
	interval.tv_usec=0;

	new.it_interval=interval;
	new.it_value=interval;
	setitimer(ITIMER_VIRTUAL,&new,&old);

	while(siginfo==1);

	close(fifo);
	close(globalfd);
	return 0;
}
