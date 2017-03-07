
from.. type_registry import _tr
from ..timestamp import Timestamp
from ..context import Context
from ..utils import displayTable

Clock = _tr.Clock
RootClock = _tr.RootClock
Workbook = _tr.Workbook
WorkItemOpenEvent = _tr.WorkItemOpenEvent

def main(_odb):
    
    with _odb: 
        clock = Clock('Workflow').write()
        clock.setParent(RootClock.get('Main', db=_odb))

        wb1 = Workbook('Customer123')
        hd  = Workbook('Helpdesk')
        wb3 = Workbook('Customer.joe')
        fd  = Workbook('Fire Department')

        startOfDay = Timestamp()

        ev0 = WorkItemOpenEvent(message='Help, I forgot my password',
                                book1=wb1,
                                book2=hd).write()

        ev1 = WorkItemOpenEvent(message='Help! My computer is smoking',
                                book1=wb3,
                                book2=hd).write()

        noon = Timestamp()

        ev2 = WorkItemOpenEvent(message='Help! My computer is on fire!',
                                book1=wb3,
                                book2=hd,
                                ticket=ev1.ticket(),
                                amends=ev1).write()

        ev3 = WorkItemOpenEvent(message='My mouse is broken',
                                book1=wb1,
                                book2=hd).write()

        t3 = Timestamp()

        ticket=ev3.ticket()

        ticket.addMessage('Actually, only the right mouse button is bad, so just replace that. Thx!')

        ev4 = ev2.ticket().transfer(book1=hd, book2=fd, action='escalate')

        ev2.ticket().addMessage('Um, the smoke is making it hard to see my desk.')

        t4 = Timestamp()

        ev0.cancel()

        endOfDay = Timestamp()

    def status():
        print 'Status:'
        for wb in wb1, wb3, hd, fd:
            print '    Workbook %s:' % wb.meta.name()
            for ticket in wb.tickets():
                print '        %s' % ticket.str(source=wb)
                for event in ticket.activeEvents():
                    print '           %s' % event.str(source=wb)
            print


    # In[186]:

    with Context({clock.cutoffs: startOfDay}) as ctx:
        status()


    # In[187]:

    with Context({clock.cutoffs: noon}) as ctx:
        status()


    # In[188]:

    with Context({clock.cutoffs: t3}) as ctx:
        status()


    # In[189]:

    with Context({clock.cutoffs: t4}) as ctx:
        status()


    # In[190]:

    with Context({clock.cutoffs: endOfDay}) as ctx:
        status()


    # In[191]:

    ticket = fd.tickets()[0]

    displayTable(ticket._allEvents())

    print

    displayTable(ticket.activeEvents())


    # In[192]:

    print ticket.sourceBook().refData()

    print ticket.sourceBook().clock().str()
    print ticket.sourceBook().refData().clock().str()

