from hl7apy import parser
from hl7apy.core import Group, Segment
from hl7apy.exceptions import UnsupportedVersion



hl7  = ""

try:
    msg = parser.parse_message(hl7.replace('\n', '\r'), find_groups=True, validation_level=2)
except UnsupportedVersion:
    msg = parser.parse_message(hl7.replace('\n', '\r'), find_groups=True, validation_level=2)
   
    
indent = "    "
indent_seg = "    "
indent_fld = "        "

def subgroup (group, indent):
    indent = indent + "    "
    print (indent , group)
    for group_segment in group.children:
        if isinstance(group_segment, Group):
            subgroup (group_segment)
        else: 
            print(indent_seg, indent ,group_segment)
            for attribute in group_segment.children:
                print(indent_fld, indent ,attribute, attribute.value)


def showmsg (msg):
    print(msg.children[1])
    for segment in msg.children:
        if isinstance(segment, Segment):
            print (indent ,segment)
            for attribute in segment.children:
                print(indent_fld, indent, attribute, attribute.value)
        if isinstance(segment, Group):
            for group in segment.children:
                print (indent,group)
                for group_segment in group.children:
                    if isinstance (group_segment, Group): 
                        subgroup (group_segment, indent)
                    else:
                        print(indent_seg, indent ,group_segment)
                        for attribute in group_segment.children:
                            print(indent_fld, indent, attribute, attribute.value)

showmsg (msg)     


